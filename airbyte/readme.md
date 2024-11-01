tar -xvzf abctl-v0.19.0-linux-amd64.tar.gz
mv abctl-v0.19.0-linux-amd64 abctl
chmod +x abctl/abctl
sudo mv abctl /usr/local/bin
nano ~/.bashrc
add line
    export PATH="$PATH:/usr/local/bin/abctl"
source ~/.bashrc

refer the following for reference:
https://docs.airbyte.com/using-airbyte/getting-started/oss-quickstart