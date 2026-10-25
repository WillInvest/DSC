"""Provider address changes, distinct from configurator implementation upgrades.

The official ConfiguratorUpdated events contain implementation addresses. Actual
routing follows AddressSet and ProxyCreated for the configurator registry key.
"""

def change(name,args,version):
    identifier=('LENDING_POOL_CONFIGURATOR' if version=='v2' else 'POOL_CONFIGURATOR').encode().ljust(32,b'\0')
    if bytes(args['id'])!=identifier or name=='AddressSet' and args.get('hasProxy',False):
        return None
    return (args.get('proxyAddress') or args['newAddress']).lower()
