package resolver

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openshift/library-go/pkg/network/resolver/dns"

	v1 "k8s.io/api/core/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// ResolverOption defines the functional option type for resolver
type ResolverOption func(*resolver) *resolver

func WithTimeout(timeout time.Duration) func(*resolver) {
	return func(r *resolver) {
		r.timeout = timeout
	}
}

func WithPeriod(period time.Duration) func(*resolver) {
	return func(r *resolver) {
		r.period = period
	}
}

// resolver to store API server IP addresses
type resolver struct {
	mu    sync.Mutex
	cache []net.IP // API server IP addresses
	host  string   // API server configured hostname
	port  string   // API server configured port

	// period to poll the API server
	period time.Duration
	// time after a network error is detected to fallback to the default resolver
	timeout time.Duration
	// RESTclient configuration
	client *rest.RESTClient
}

// NewResolver returns an in memory net.Resolver that resolves the API server
// Host name with the addresses obtained from the API server published Endpoints
// resources.
// The resolver polls periodically the API server to refresh the local cache.
// The resolver will fall back to the default golang resolver if:
// - Is not able to obtain the API server Endpoints.
// - The configured API server host name is not resolvable via DNS, per example,
//   is not an IP address or is resolved via /etc/hosts.
// - The configured API server URL has a different port
//   than the one used in the Endpoints.
func NewResolver(ctx context.Context, c *rest.Config, options ...ResolverOption) (*net.Resolver, error) {
	config := *c
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}

	host, port, err := getHostPort(config.Host)
	if err != nil {
		return nil, err
	}

	if netutils.ParseIPSloppy(host) != nil {
		return net.DefaultResolver, fmt.Errorf("APIServerResolver only works for domain names")
	}

	// defaulting
	r := &resolver{
		host:    host,
		port:    port,
		period:  10 * time.Second,
		timeout: 300 * time.Second,
	}

	// options
	for _, o := range options {
		o(r)
	}

	f := &dns.MemResolver{
		LookupIP: func(ctx context.Context, network, host string) ([]net.IP, error) {
			return r.lookupIP(ctx, network, host)
		},
	}

	resolver := dns.NewMemoryResolver(f)
	config.Resolver = resolver

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}

	r.client = client
	// Initialize cache and close idle connections so next connections goes
	// directly to one of the API server published IPs, this is useful for cases
	// that use a load balancer for bootstrapping
	r.refreshCache(ctx)
	utilnet.CloseIdleConnectionsFor(r.client.Client.Transport)
	r.Start(ctx)
	return resolver, nil
}

func setConfigDefaults(config *rest.Config) error {
	gv := v1.SchemeGroupVersion
	config.GroupVersion = &gv
	config.APIPath = "/api"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	return nil
}

func (r *resolver) lookupIP(ctx context.Context, network, host string) ([]net.IP, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Use the default resolver if is not trying to resolve the configured API server hostname
	if !strings.HasPrefix(host, r.host) {
		klog.V(7).Infof("use default resolver for host %s, different than API server hostname %s", host, r.host)
		return net.DefaultResolver.LookupIP(ctx, network, host)
	}

	// Use the default resolver if the cache is empty.
	if len(r.cache) == 0 {
		klog.V(7).Infof("use default resolver for host %s, cache is empty", host)
		return net.DefaultResolver.LookupIP(ctx, network, host)
	}

	// Return the IP addresses from the cache.
	result := make([]net.IP, len(r.cache))
	copy(result, r.cache)
	klog.V(7).Infof("host %s resolves to %v", host, result)
	return result, nil
}

// refreshCache refresh the cache with the API server IP addresses.
// If it can not refresh the IPs because of network errors during
// a predefined time, it falls back to the default resolver.
// If it can not refresh the IPs because of other type of errors, per
// example, it is able to connect to the API server but this is not ready
// or is not able to reply, it shuffles the IPs so it will retry randomly.
// If there are no errors, local IP addresses are returned first, so it
// favors direct connectivity.
func (r *resolver) refreshCache(ctx context.Context) error {
	// Kubernetes conformance clusters require: The cluster MUST have a service
	// named "kubernetes" on the default namespace referencing the API servers.
	// The "kubernetes.default" service MUST have Endpoints and EndpointSlices
	// pointing to each API server instance.
	// Endpoints managed by API servers are removed if they API server is not ready.
	endpoint := &v1.Endpoints{}
	err := r.client.Get().
		Resource("endpoints").
		Namespace("default").
		Name("kubernetes").
		Do(ctx).
		Into(endpoint)
	if err != nil {
		klog.V(7).Infof("Error getting apiserver addresses from Endpoints: %v", err)
		return err
	}
	// Get IPs from the Endpoint.
	ips := []net.IP{}
	supported := true
	for _, ss := range endpoint.Subsets {
		for _, e := range ss.Addresses {
			ips = append(ips, netutils.ParseIPSloppy(e.IP))
		}
		if len(ss.Ports) != 1 {
			klog.Info("Unsupported api servers endpoints with multiple ports")
			supported = false
		} else if strconv.Itoa(int(ss.Ports[0].Port)) != r.port {
			klog.Info("Unsupported api servers host with different port")
			supported = false
		}
	}
	// Do nothing if there are no IPs published or is an unsupported configuration.
	if len(ips) == 0 || !supported {
		return nil
	}

	// Update the cache and exit
	if len(ips) == 1 {
		r.mu.Lock()
		r.cache = []net.IP{ips[0]}
		r.mu.Unlock()
		return nil
	}

	// Shuffle the ips so different clients don't end in the same API server.
	rand.Shuffle(len(ips), func(i, j int) {
		ips[i], ips[j] = ips[j], ips[i]
	})
	// Favor local, returning it first because dialParallel races two copies of
	// dialSerial, giving the first a head start. It returns the first
	// established connection and closes the others.
	localAddresses := getLocalAddressSet()
	for _, ip := range ips {
		if localAddresses.Has(ip) {
			moveToFront(ip, ips)
			break
		}
	}

	r.mu.Lock()
	r.cache = make([]net.IP, len(ips))
	copy(r.cache, ips)
	r.mu.Unlock()
	return nil
}

// Start starts a loop to get the API server Endpoints. The resolver uses the
// same dialer it feeds, so it will benefit from the resilience it provides.
func (r *resolver) Start(ctx context.Context) {
	klog.Info("Starting in memory apiserver resolver ...")
	go func() {
		// Refresh the cache periodically
		tick := time.Tick(r.period)
		lastNetworkError := time.Time{}
		for {
			select {
			case <-tick:
				err := r.refreshCache(ctx)
				// cache refreshed successfully, reset network error timestamp and continue
				if err == nil {
					lastNetworkError = time.Time{}
					continue
				}
				// nothing to do here, continue
				if len(r.cache) == 0 {
					continue
				}
				// error handling
				_, netErrorOk := err.(net.Error)
				if netErrorOk {
					// If is the first network error record the timestamp and
					// retry.
					if lastNetworkError.IsZero() {
						lastNetworkError = time.Now()
						continue
					}
					// If there are several network errors check the specified
					// timeout and clean the cache so we fall back to the
					// default resolver and we can start over.
					if time.Now().Sub(lastNetworkError) > r.timeout {
						klog.V(2).Infof("Falling back to default resolver, too many errors to connect to %s:%s on %v : %v", r.host, r.port, r.cache, err)
						lastNetworkError = time.Time{}
						r.mu.Lock()
						r.cache = []net.IP{}
						r.mu.Unlock()
						continue
					}
				}

				// no network error, probably the connection succeeded but the
				// server is not ready, shuffle the IPs so next connection tries
				// a different server (eventually).
				lastNetworkError = time.Time{}
				r.mu.Lock()
				rand.Shuffle(len(r.cache), func(i, j int) {
					r.cache[i], r.cache[j] = r.cache[j], r.cache[i]
				})
				r.mu.Unlock()

			case <-ctx.Done():
				return
			}
		}
	}()
}

// getHostPort returns the host and port from an URL defaulting http and https ports
func getHostPort(h string) (string, string, error) {
	url, err := url.Parse(h)
	if err != nil {
		return "", "", err
	}

	port := url.Port()
	if port == "" {
		switch url.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return "", "", fmt.Errorf("Unsupported URL scheme")
		}
	}
	return url.Hostname(), port, nil
}

// getLocalAddrs returns a set with all network addresses on the local system
func getLocalAddressSet() netutils.IPSet {
	localAddrs := netutils.IPSet{}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		klog.InfoS("Error getting local addresses", "error", err)
		return localAddrs
	}

	for _, addr := range addrs {
		ip, _, err := netutils.ParseCIDRSloppy(addr.String())
		if err != nil {
			klog.InfoS("Error getting local addresses", "address", addr.String(), "error", err)
			continue
		}
		localAddrs.Insert(ip)
	}
	return localAddrs
}

// https://github.com/golang/go/wiki/SliceTricks#move-to-front-or-prepend-if-not-present-in-place-if-possible
// moveToFront moves needle to the front of haystack, in place if possible.
func moveToFront(needle net.IP, haystack []net.IP) []net.IP {
	if len(haystack) != 0 && haystack[0].Equal(needle) {
		return haystack
	}
	prev := needle
	for i, elem := range haystack {
		switch {
		case i == 0:
			haystack[0] = needle
			prev = elem
		case elem.Equal(needle):
			haystack[i] = prev
			return haystack
		default:
			haystack[i] = prev
			prev = elem
		}
	}
	return append(haystack, prev)
}
