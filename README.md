After cloning:

# Step 1

`./download-element-apps.sh`

# Step 2

Configure nginx

# Step 3 (if synapse if showing permission errors for `/data/media_store`)

```
mkdir config/synapse/media_store
sudo chown -R 991:991 ./config/synapse
sudo chmod -R u+rwX ./config/synapse
```