defmodule Commands do
  def get_command(args) do
    if length(args) == 1 do
      case Store.get(hd(args)) do
        {:ok, value} ->
          response = "$#{byte_size(value)}\r\n#{value}\r\n"
          {:ok, response}

        {:error, :not_found} ->
          {:ok, "$-1\r\n"}
      end
    else
      {:error, "GET command expects exactly one argument"}
    end
  end

  def set_command(args) do
    if length(args) == 2 do
      case Store.set(hd(args), Enum.at(args, 1)) do
        {:ok, _value} ->
          response = "+OK\r\n"
          {:ok, response}

        {:error, :insert_failed} ->
          {:error, "Failed to set value: #{hd(args)}"}
      end
    end
  end

  def ping_command() do
    {:ok, "+PONG\r\n"}
  end

  def echo_command(args) do
    if length(args) == 1 do
      response = "$#{byte_size(hd(args))}\r\n#{hd(args)}\r\n"
      {:ok, response}
    else
      {:error, "ECHO command expects exactly one argument"}
    end
  end
end
