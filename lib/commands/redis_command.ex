defmodule RedisCommand do
  alias Commands.Lrange
  alias Commands.{Set, Ping, Echo, Get, Rpush}
  @moduledoc """
  Behaviour for Redis command implementations.
  All command functions must return {:ok, response} or {:error, reason}.
  """

  @doc """
  Execute a Redis command with the given arguments.
  Returns {:ok, redis_response} or {:error, error_message}.
  """
  @callback execute(args :: [String.t()]) :: {:ok, String.t()} | {:error, String.t()}

  # Convenience functions for backward compatibility
  def get_command(args), do: Get.execute(args)
  @spec set_command(any()) :: {:error, <<_::64, _::_*8>>} | {:ok, <<_::40>>}
  def set_command(args), do: Set.execute(args)
  def ping_command(), do: Ping.execute([])
  def echo_command(args), do: Echo.execute(args)
  def rpush_command(args), do: Rpush.execute(args)
  def lrange_command(args), do: Lrange.execute(args)
end
