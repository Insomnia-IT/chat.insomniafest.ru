After cloning:

# Step 1

`./bootstrap-env-file.sh` - will generate an .env file with random strings for all parameters in the .env.example template

# Step 2

`./download-element-apps.sh` - this will create an .env file

# Step 3

Configure nginx

# Step 4

`docker compose up -d`

This will create the database and generate the `matrix.chat.signing.key`  file.

# If synapse if showing permission errors for `/data/media_store`

```
mkdir config/synapse/media_store
sudo chown -R 991:991 ./config/synapse
sudo chmod -R u+rwX ./config/synapse
```
