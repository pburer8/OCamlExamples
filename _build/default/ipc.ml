let send_frame flow buf =
  let len = Cstruct.length buf in
  let header = Cstruct.create 4 in
  Cstruct.BE.set_uint32 header 0 (Int32.of_int len);
  Eio.Flow.write flow [header; buf]

let recv_frame flow =
  let header = Cstruct.create 4 in
  Eio.Flow.read_exact flow header;
  let len = Int32.to_int (Cstruct.BE.get_uint32 header 0) in
  let max_frame = 16 * 1024 * 1024 in  (* 16 MB ceiling *)
  if len < 0 || len > max_frame then raise (Invalid_argument (Printf.sprintf "frame too large: %d" len));
  let body = Cstruct.create len in
  Eio.Flow.read_exact flow body;
  body

module Socket =
struct
  type s_type = PUB | SUB

  let send conn msg = send_frame conn (Cstruct.of_string (msg))

  let recv conn =
    let frame = recv_frame conn in
    let msg = Cstruct.to_string frame in
    msg
end

module Publisher = 
struct
  include Socket

  type s = {
    socket_type : Socket.s_type;
    mutex : Eio.Mutex.t;
    path : string;
    stream : string Eio.Stream.t;
    mutable subs : string list
  }

  let create s p = {
    socket_type = s; mutex = Eio.Mutex.create (); path = p; stream = Eio.Stream.create max_int; subs = []
  }

  let path pub = pub.path

  let add_subscriber pub path =
    Eio.Mutex.lock pub.mutex;
    pub.subs <- pub.subs @ [path];
    Eio.Mutex.unlock pub.mutex

  let run_server_recv sock mu cond server_ready env =
    let accept_loop server sw sock =
      while true do
        Eio.Net.accept_fork server ~sw ~on_error:raise
          (fun conn _addr ->
            let rec loop () =
              try
                let msg = recv conn in
                Eio.traceln "Server received: %S" msg;
                Eio.Stream.add sock.stream msg;
                loop ()
              with End_of_file -> ()
            in
            loop ())
      done
    in

    Eio.Switch.run @@ fun sw ->
      let net = Eio.Stdenv.net env in
      let server = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 (`Unix sock.path) in

      Mutex.lock mu;
      server_ready := true;
      Condition.broadcast cond;
      Mutex.unlock mu;

      accept_loop server sw sock
  
  let run_server_send sock env =
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let connections = List.map (fun path ->
      Eio.Net.connect ~sw net (`Unix path)) sock.subs in

    while true do 
      let msg = Eio.Stream.take sock.stream in
      List.iter (fun conn -> send conn msg) connections;
    done
  
  let run_server sock mu cond server_ready env =
    Eio.Fiber.both
      (fun () -> run_server_recv sock mu cond server_ready env)
      (fun () -> run_server_send sock env)
    
end

module Subscriber =
struct
  include Socket

  type s = {
    socket_type : s_type;
    mutex : Eio.Mutex.t;
    path : string;
    mutable server_path : string
  }

  let create s p = {
    socket_type = s; mutex = Eio.Mutex.create (); path = p; server_path = ""
  }

  let path sub = sub.path


  let connect sub pub = 
    Eio.Mutex.lock sub.mutex;
    sub.server_path <- Publisher.path pub;
    Publisher.add_subscriber pub (path sub);
    Eio.Mutex.lock sub.mutex
    
  let run_client sock mu cond server_ready env =
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let my_path = sock.path in
    let server_path = sock.server_path in

    let listener = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 (`Unix my_path) in

    Mutex.lock mu;
    while not !server_ready do Condition.wait cond mu done;
    Mutex.unlock mu;
    let out_conn = Eio.Net.connect ~sw net (`Unix server_path) in
    Eio.traceln "Client: connected";
    send out_conn my_path;  (* actual message to broadcast *)

    while true do
      Eio.Net.accept_fork listener ~sw ~on_error:raise
        (fun in_conn _addr ->
          try
            while true do
              let msg = recv in_conn in
              Eio.traceln "Client %S: received %S" my_path msg
            done
          with End_of_file -> ())
    done
end

let () =
  let in_path1 = "/tmp/eio_in1.sock" in
  let in_path2 = "/tmp/eio_in2.sock" in
  let out_path1 = "/tmp/eio_out1.sock" in
  let out_path2 = "/tmp/eio_out2.sock" in
  
  (* Clean up any leftover socket file *)
  (try Unix.unlink in_path1 with Unix.Unix_error _ -> ());
  (try Unix.unlink in_path2 with Unix.Unix_error _ -> ());
  (try Unix.unlink out_path1 with Unix.Unix_error _ -> ());
  (try Unix.unlink out_path2 with Unix.Unix_error _ -> ());

  (* Spin up two domains: one for the server, one for the client *)
  let pool = Domainslib.Task.setup_pool ~num_domains:3 () in

  let pub = Publisher.create Socket.PUB in_path1 in
  let sub1 = Subscriber.create Socket.SUB out_path1 in
  let sub2 = Subscriber.create Socket.SUB out_path2 in

  Subscriber.connect sub1 pub;
  Subscriber.connect sub2 pub;
(* pass ready to clients, resolve to server *)

  let mu = Mutex.create () in
  let cond = Condition.create () in
  let server_ready = ref false in

  Domainslib.Task.run pool (fun () ->
  let server = Domainslib.Task.async pool (fun () ->
    Eio_main.run (Publisher.run_server pub mu cond server_ready)) in
  let client1 = Domainslib.Task.async pool (fun () ->
    Eio_main.run (Subscriber.run_client sub1 mu cond server_ready)) in
  let client2 = Domainslib.Task.async pool (fun () ->
    Eio_main.run (Subscriber.run_client sub2 mu cond server_ready)) in
  
  Domainslib.Task.await pool server;
  Domainslib.Task.await pool client1;
  Domainslib.Task.await pool client2
  
)