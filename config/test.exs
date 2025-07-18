# Test configuration - don't start the full application
import Config

# Don't start the server on port 6379 during tests
config :codecrafters_redis, start_server: false

# Configure logger for tests
config :logger, level: :warning
