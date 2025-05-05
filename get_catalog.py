from pyiceberg.catalog import load_catalog
import ipyiceberg as ipg

# Get keys from dictionary
props = ipg.cat["catalog"]["default"]
type = props["type"]
uri = props["uri"]
warehouse = props["warehouse"]
py_io = props["py-io-impl"]

def get_catalog():
    catalog = load_catalog(
        name="sqlcat",
        **{"uri": uri,
           "type": type,
           "warehouse": warehouse,
           "py-io-impl": py_io}
    )

    return catalog