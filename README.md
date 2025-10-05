# PubSub_withScanner
python pubsub
Pure python pub sub 

# Get LIST of SYMBOL to TICKER ID MAPPING AT 
https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz



# Explanation of Changes

subscribed_keys and current_ltpc_mode_keys:
self.subscribed_keys: This set keeps track of all instrument keys that should be subscribed according to your application logic.
self.current_ltpc_mode_keys: This set tracks the instrument keys that are currently subscribed to the Upstox WebSocket in ltpc mode. This helps avoid redundant subscription requests if a key is already active.
They are separate because the WebSocket might temporarily disconnect and reconnect, or a subscription command might arrive when the WebSocket isn't yet fully open.
ws_connection:
The on_open method now stores the ws object (the websocket-client instance) in self.ws_connection. This is crucial because you need this object to send subscription/unsubscription messages later.
handle_subscription_command(self, command_message):
This new method is the core logic for dynamic updates. It's registered as a subscriber to the SUBSCRIPTION_COMMAND_TOPIC in the __init__ method.
When a message arrives, it parses the action ('ADD' or 'REMOVE') and the instrument_key(s).
It updates the self.subscribed_keys set accordingly.
It then determines which keys actually need to be sent as 'sub' or 'unsub' commands to the WebSocket (i.e., keys that are in self.subscribed_keys but not self.current_ltpc_mode_keys, or vice versa).
Finally, it calls _send_subscription_message to send the actual command to the Upstox WebSocket.
_send_subscription_message:
A helper method to standardize sending subscription messages. It checks if the ws_connection is active before sending.
Simulated "Other App" (if __name__ == "__main__": block):
A command_pubsub_client instance is created (could be a separate process using NetPubSub).
It publishes messages to SUBSCRIPTION_COMMAND_TOPIC with action and instrument_key/instrument_keys payloads. This simulates how your external application would trigger subscription changes.
How it works with mpubsub
The UpstoxDataPublisher thread runs and connects to the Upstox WSS.
Simultaneously, mpubsub is running, managing topics and subscribers.
The UpstoxDataPublisher is subscribed to SUBSCRIPTION_COMMAND_TOPIC.
When your "other app" publishes a command to SUBSCRIPTION_COMMAND_TOPIC, mpubsub routes it to handle_subscription_command in the UpstoxDataPublisher instance.
handle_subscription_command then updates the internal state and sends the required "sub" or "unsub" message directly to the Upstox WebSocket via self.ws_connection.send(). 


This setup effectively decouples the control logic (from the "other app") from the data streaming logic, allowing for dynamic updates without restarting the main WebSocket connection.






Here is a Python client application that can subscribe to LTPC data from the mpubsub broker and send subscription commands to the Upstox WSS publisher. This client code is designed to run in a separate process from the main publisher, showcasing the full power of mpubsub's inter-process communication capabilities.





pip install upstox-python-sdk mpubsub protobuf


