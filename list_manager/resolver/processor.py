import asyncio
import dns.resolver
from dns.asyncresolver import Resolver
from .abstract import AsyncBatchProcessor
from .utils import ResolverSet
from list_manager import log

class AsyncResolveProcessor(AsyncBatchProcessor):
    resolver = Resolver()
    resolver.nameservers = ["8.8.8.8", "8.8.4.4"]
    resolver.rotate = True

    def __init__(self, retries=3, lifetime=6, **kwargs):
        self.lifetime = lifetime
        self.tcp = False
        self.retries = retries
        super().__init__(**kwargs)

    @classmethod
    def config_resolver(cls, nameservers:list==["8.8.8.8", "8.8.4.4"], rotate=True):
        cls.resolver.nameservers = nameservers
        cls.resolver.rotate = rotate
        
    async def process(self, domain: str) -> (ResolverSet, str):
        lifetime = self.resolver.lifetime
        retries = 0
        delay = 0

        while retries <= self.retries:
            try:
                _ = await self.resolver.resolve(domain, "A", lifetime=lifetime, tcp=self.tcp)
                log.info(f"{domain} resolved")
                return (ResolverSet.resolvable, domain)
            except dns.resolver.NXDOMAIN as e:
                log.debug(f"{e}")
                return (ResolverSet.unresolvable, domain)
            except dns.resolver.NoAnswer as e:
                log.debug(f"{e}")
                return (ResolverSet.unresolvable, domain)  # No answer from server, consider as deprecated
            except dns.resolver.NoNameservers as e:
                log.debug(f"{e}")
                return (ResolverSet.nameServerError, domain)  # No non-broken nameservers are available to answer the question.
            except dns.resolver.LifetimeTimeout as e:
                log.debug(f"{e}")
               
                if retries == self.retries:
                    log.error(f"retries exhausted for domain {domain}")
                    return (ResolverSet.timeout, domain)

                await asyncio.sleep(delay)
                delay += 1
                retries += 1
                lifetime *= delay
                continue
            except dns.exception.DNSException as e:
                log.debug(f"{e}")
                return (ResolverSet.dnsError, domain)
            except Exception as e:
                log.debug(f"{e}")
                return (ResolverSet.error, domain) # Other errors, consider as deprecated

        # should not get here
        return (ResolverSet.none, domain)