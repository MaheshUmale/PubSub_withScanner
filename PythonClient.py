import logging
import time
from mpubsub import mpubsub

# --- Configuration ---
# IMPORTANT: Use the same topic names and address as your main Upstox publisher.
# If the publisher is running locally, 'localhost' is fine.
PUBSUB_BROKER_ADDRESS = ('localhost', 5000)
SUBSCRIPTION_COMMAND_TOPIC = 'subscription_commands'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PlottingApp:
    def __init__(self, broker_address):
        """Initializes the plotting client and connects to the mpubsub broker."""
        self.pubsub_client = mpubsub.NetPubSub(broker_address[0], broker_address[1])
        self.data_history = {}  # Store incoming data for plotting (or other purposes)

    def on_data(self, topic, message):
        """
        Callback function to process incoming LTPC data.
        This is where you would update your real-time plots.
        """
        instrument_key = message['instrument_key']
        ltp = message['ltp']
        timestamp = message['timestamp']

        # Store the data
        if instrument_key not in self.data_history:
            self.data_history[instrument_key] = []
        self.data_history[instrument_key].append({'timestamp': timestamp, 'ltp': ltp})

        logging.info(f"Received data for {instrument_key}: LTP={ltp} at {timestamp}")

        # Example plotting logic (in a real app, this would update a GUI)
        # For simplicity, we just print the last 5 data points
        last_data = self.data_history[instrument_key][-5:]
        logging.info(f"Last 5 data points for {instrument_key}: {last_data}")

    def send_subscription_request(self, action, instrument_keys):
        """
        Sends a command to the publisher to add or remove tickers.
        """
        if not isinstance(instrument_keys, list):
            instrument_keys = [instrument_keys]
        
        command = {
            'action': action,
            'instrument_keys': instrument_keys,
        }
        self.pubsub_client.publish(SUBSCRIPTION_COMMAND_TOPIC, command)
        logging.info(f"Sent command to publisher: {command}")

    def run(self):
        """Starts the client and its subscription loop."""
        logging.info(f"Connecting to mpubsub broker at {PUBSUB_BROKER_ADDRESS}...")
        self.pubsub_client.connect()
        logging.info("Connected to broker. Starting client loop.")
        self.pubsub_client.run()
        logging.info("Client application terminated.")

    def close(self):
        """Closes the client connection."""
        self.pubsub_client.close()
        logging.info("Client disconnected from broker.")

if __name__ == "__main__":
    try:
        app = PlottingApp(PUBSUB_BROKER_ADDRESS)

        # 1. Subscribe to an initial set of topics.
        initial_topics = [
            "NSE_INDEX|Nifty 50",
            "NSE_EQ|INE002A01018",  # Reliance Industries Ltd
        ]
        for topic in initial_topics:
            app.pubsub_client.subscribe(topic, app.on_data)

        # Start the client loop in a separate thread to keep the main thread free
        # for sending commands.
        client_thread = threading.Thread(target=app.run, daemon=True)
        client_thread.start()

        time.sleep(5)  # Wait for initial data to start flowing

        # 2. Example: Send a request to add a new ticker.
        new_ticker_to_add = "NSE_EQ|INE018A01016"  # HDFC Bank
        app.send_subscription_request('ADD', new_ticker_to_add)
        app.pubsub_client.subscribe(new_ticker_to_add, app.on_data)
        time.sleep(5)

        # 3. Example: Send a request to remove an existing ticker.
        ticker_to_remove = "NSE_EQ|INE002A01018"  # Reliance
        app.send_subscription_request('REMOVE', ticker_to_remove)
        app.pubsub_client.unsubscribe(ticker_to_remove, app.on_data)
        time.sleep(5)
        
        # 4. Example: Add multiple tickers at once.
        tickers_to_add_multi = [
            "NSE_EQ|INE669E01016", # IRCTC
            "NSE_EQ|INE118D01018"  # PowerGrid
        ]
        app.send_subscription_request('ADD', tickers_to_add_multi)
        for ticker in tickers_to_add_multi:
            app.pubsub_client.subscribe(ticker, app.on_data)
        
        # Keep the main process alive
        logging.info("Client is running. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down client...")
    finally:
        app.close()
