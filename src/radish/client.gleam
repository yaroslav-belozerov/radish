import gleam/string
import gleam/io
import gleam/result
import gleam/otp/static_supervisor
import gleam/bit_array
import gleam/erlang/process
import gleam/otp/actor

import radish/decoder.{decode}
import radish/error
import radish/resp.{type Value}
import radish/tcp

import lifeguard
import mug.{type Error}

pub type Message {
  Command(BitArray, process.Subject(Result(List(Value), error.Error)), Int)
  BlockingCommand(
    BitArray,
    process.Subject(Result(List(Value), error.Error)),
    Int,
  )
  ReceiveForever(process.Subject(Result(List(Value), error.Error)), Int)
}

pub type Client =
  process.Subject(lifeguard.PoolMsg(Message))

pub fn start(
  host: String,
  port: Int,
  timeout: Int,
  pool_size: Int,
  hello_cmd: BitArray,
) -> Result(Client, actor.StartError) {
  let name = process.new_name("radish_pool")
  let subject = process.named_subject(name)
  case lifeguard.new_with_initialiser(
    name,
    timeout,
    fn (_) { init_worker(host, port, timeout, hello_cmd) }
  )
  |> lifeguard.on_message(handle_message)
  |> lifeguard.size(pool_size)
  |> lifeguard.start(timeout) {
    Ok(_) -> Ok(subject)
    Error(err) -> Error(err)
  }
}

fn init_worker(
  host: String,
  port: Int,
  timeout: Int,
  hello_cmd: BitArray,
) -> Result(lifeguard.Initialised(mug.Socket, Message), String) {
  use socket <- result.try(
    tcp.connect(host, port, timeout)
    |> result.map_error(fn(_) { "Unable to connect to Redis server" })
  )

  case tcp.send(socket, hello_cmd) {
    Ok(Nil) -> {
      let selector = tcp.new_selector()
      case receive(socket, selector, <<>>, now(), timeout) {
        Ok([resp.SimpleError(msg)]) | Ok([resp.BulkError(msg)]) -> {
          let _ = mug.shutdown(socket)
          Error("Authentication failed: " <> msg)
        }
        Ok(_) -> Ok(lifeguard.initialised(socket))
        Error(err) -> {
          let _ = mug.shutdown(socket)
          Error("Failed to authenticate: " <> error_to_string(err))
        }
      }
    }
    Error(_) -> {
      let _ = mug.shutdown(socket)
      Error("Failed to send HELLO command")
    }
  }
}

fn error_to_string(err: error.Error) -> String {
  case err {
    error.NotFound -> "Not found"
    error.RESPError -> "Invalid response"
    error.ActorError -> "Pool error"
    error.ConnectionError -> "Connection error"
    error.TCPError(_) -> "Network error"
    error.ServerError(msg) -> msg
  }
}


fn handle_message(socket: mug.Socket, msg: Message) {
  case msg {
    Command(cmd, reply_with, timeout) -> {
      case tcp.send(socket, cmd) {
        Ok(Nil) -> {
          let selector = tcp.new_selector()
          case receive(socket, selector, <<>>, now(), timeout) {
            Ok(reply) -> {
              actor.send(reply_with, Ok(reply))
              actor.continue(socket)
            }

            Error(error) -> {
              let _ = mug.shutdown(socket)
              actor.send(reply_with, Error(error))
              actor.stop_abnormal("TCP Error")
            }
          }
        }

        Error(error) -> {
          let _ = mug.shutdown(socket)
          actor.send(reply_with, Error(error.TCPError(error)))
          actor.stop_abnormal("TCP Error")
        }
      }
    }

    BlockingCommand(cmd, reply_with, timeout) -> {
      case tcp.send(socket, cmd) {
        Ok(Nil) -> {
          let selector = tcp.new_selector()

          case receive_forever(socket, selector, <<>>, now(), timeout) {
            Ok(reply) -> {
              actor.send(reply_with, Ok(reply))
              actor.continue(socket)
            }

            Error(error) -> {
              let _ = mug.shutdown(socket)
              actor.send(reply_with, Error(error))
              actor.stop_abnormal("TCP Error")
            }
          }
        }

        Error(error) -> {
          let _ = mug.shutdown(socket)
          actor.send(reply_with, Error(error.TCPError(error)))
          actor.stop_abnormal("TCP Error")
        }
      }
    }

    ReceiveForever(reply_with, timeout) -> {
      let selector = tcp.new_selector()

      case receive_forever(socket, selector, <<>>, now(), timeout) {
        Ok(reply) -> {
          actor.send(reply_with, Ok(reply))
          actor.continue(socket)
        }

        Error(error) -> {
          let _ = mug.shutdown(socket)
          actor.send(reply_with, Error(error))
          actor.stop_abnormal("TCP Error")
        }
      }
    }
  }
}

fn receive(
  socket: mug.Socket,
  selector: process.Selector(Result(BitArray, mug.Error)),
  storage: BitArray,
  start_time: Int,
  timeout: Int,
) {
  case decode(storage) {
    Ok(value) -> Ok(value)
    Error(error) -> {
      case now() - start_time >= timeout * 1_000_000 {
        True -> Error(error)
        False ->
          case tcp.receive(socket, selector, timeout) {
            Error(tcp_error) -> Error(error.TCPError(tcp_error))
            Ok(packet) ->
              receive(
                socket,
                selector,
                bit_array.append(storage, packet),
                start_time,
                timeout,
              )
          }
      }
    }
  }
}

fn receive_forever(
  socket: mug.Socket,
  selector: process.Selector(Result(BitArray, mug.Error)),
  storage: BitArray,
  start_time: Int,
  timeout: Int,
) {
  case decode(storage) {
    Ok(value) -> Ok(value)

    Error(error) if timeout != 0 -> {
      case now() - start_time >= timeout * 1_000_000 {
        True -> Error(error)
        False ->
          case tcp.receive_forever(socket, selector) {
            Error(tcp_error) -> Error(error.TCPError(tcp_error))
            Ok(packet) ->
              receive_forever(
                socket,
                selector,
                bit_array.append(storage, packet),
                start_time,
                timeout,
              )
          }
      }
    }

    Error(_) -> {
      case tcp.receive_forever(socket, selector) {
        Error(tcp_error) -> Error(error.TCPError(tcp_error))
        Ok(packet) ->
          receive_forever(
            socket,
            selector,
            bit_array.append(storage, packet),
            start_time,
            timeout,
          )
      }
    }
  }
}

@external(erlang, "erlang", "monotonic_time")
fn now() -> Int
