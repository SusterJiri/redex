defmodule RedisCommand do
  alias Commands.{Set, Ping, Echo, Get, Rpush, Lpush, Lrange, Llen, Lpop, Blpop, Type, Xadd, Xrange, Xread, Incr}
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
  def set_command(args), do: Set.execute(args)
  def ping_command(), do: Ping.execute([])
  def echo_command(args), do: Echo.execute(args)
  def rpush_command(args), do: Rpush.execute(args)
  def lpush_command(args), do: Lpush.execute(args)
  def lrange_command(args), do: Lrange.execute(args)
  def llen_command(args), do: Llen.execute(args)
  def lpop_command(args), do: Lpop.execute(args)
  def blpop_command(args), do: Blpop.execute(args)
  def type_command(args), do: Type.execute(args)
  def xadd_command(args), do: Xadd.execute(args)
  def xrange_command(args), do: Xrange.execute(args)
  def xread_command(args), do: Xread.execute(args)
  def incr_command(args), do: Incr.execute(args)
end
