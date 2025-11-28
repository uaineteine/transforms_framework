import adlfs
import polars as pl

fs = adlfs.AzureBlobFileSystem(
    account_name=account_name,
    tenant_id="your-tenant-id",          # extract from tenant_endpoint
    client_id=client_id,
    client_secret=client_secret
)

# Example: read a CSV from container 'test'
with fs.open("test/mydata.csv") as f:
    df = pl.read_csv(f)

print(df.head())
