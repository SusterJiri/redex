defmodule Store do

  def setup_store() do
    :ets.new(:redis_store, [:named_table, :set, :public, read_concurrency: true])
    {:ok, :redis_store}
  end

  def get(key) do
    case :ets.lookup(:redis_store, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  def set(key, value) do
    case :ets.insert(:redis_store, {key, value}) do
      true -> {:ok, value}
      false -> {:error, :insert_failed}  # Very rare, but possible
    end
  end

end
