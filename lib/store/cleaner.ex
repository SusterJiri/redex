defmodule Store.Cleaner do
  use GenServer
  require Logger

  @cleanup_interval 10_000  # Run cleanup every 10 seconds

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(state) do
    # Schedule first cleanup
    schedule_cleanup()
    Logger.info("Store.Cleaner started")
    {:ok, state}
  end

  def handle_info(:cleanup, state) do
    cleaned_count = clean_expired_keys()
    if cleaned_count > 0 do
      Logger.info("Cleaned #{cleaned_count} expired keys")
    end

    # Schedule next cleanup
    schedule_cleanup()
    {:noreply, state}
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval)
  end

  defp clean_expired_keys() do
    now = :os.system_time(:millisecond)

    expired_keys = :ets.select(:redis_store, [
      {{:'$1', {:'$2', :'$3'}},
       [{:andalso, {:is_integer, :'$3'}, {:<, :'$3', now}}],
       [:'$1']}
    ])

    Enum.each(expired_keys, &:ets.delete(:redis_store, &1))

    length(expired_keys)
  end
end
