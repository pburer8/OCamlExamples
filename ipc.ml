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

  type t = {
    socket_type : s_type;
    mutex : Eio.Mutex.t;
    mutable pub_paths : string list;
    mutable sub_paths : string list;
    stream : string Eio.Stream.t
  }

  let create s = {
    socket_type = s; mutex = Eio.Mutex.create (); pub_paths = []; sub_paths = []; stream = Eio.Stream.create max_int
  }

  let connect pub sub path =
    Eio.Mutex.lock pub.mutex;
    pub.pub_paths <- pub.pub_paths @ [path];
    Eio.Mutex.unlock pub.mutex;
    Eio.Mutex.lock sub.mutex;
    sub.sub_paths <- sub.sub_paths @ [path];
    Eio.Mutex.unlock sub.mutex

  let send conn msg = send_frame conn (Cstruct.of_string (msg))

  let recv conn =
    let frame = recv_frame conn in
    let msg = Cstruct.to_string frame in
    msg

  

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
      let servers = List.map (fun path ->
        Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 (`Unix path)
      ) sock.sub_paths in

      Mutex.lock mu;
      server_ready := true;
      Condition.broadcast cond;
      Mutex.unlock mu;

      Eio.Fiber.all (List.map (fun server -> fun () -> accept_loop server sw sock) servers)
  
  let run_server_send sock env =
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let connections = List.map (fun path ->
      Eio.Net.connect ~sw net (`Unix path)) sock.pub_paths in

    while true do 
      let msg = Eio.Stream.take sock.stream in
      List.iter (fun conn -> send conn msg) connections;
    done
  
  let run_server sock mu cond server_ready env =
    Eio.Fiber.both
      (fun () -> run_server_recv sock mu cond server_ready env)
      (fun () -> run_server_send sock env)
    

  let run_client sock mu cond server_ready env =
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let my_path = List.hd sock.sub_paths in
    let server_path = List.hd sock.pub_paths in

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

  let pub = Socket.create Socket.PUB in
  let sub1 = Socket.create Socket.SUB in
  let sub2 = Socket.create Socket.SUB in

  Socket.connect pub sub1 out_path1;
  Socket.connect pub sub2 out_path2;
  Socket.connect sub1 pub in_path1;
  Socket.connect sub2 pub in_path2;
(* pass ready to clients, resolve to server *)

  let mu = Mutex.create () in
  let cond = Condition.create () in
  let server_ready = ref false in

  Domainslib.Task.run pool (fun () ->
  let server = Domainslib.Task.async pool (fun () ->
    Eio_main.run (Socket.run_server pub mu cond server_ready)) in
  let client1 = Domainslib.Task.async pool (fun () ->
    Eio_main.run (Socket.run_client sub1 mu cond server_ready)) in
  let client2 = Domainslib.Task.async pool (fun () ->
    Eio_main.run (Socket.run_client sub2 mu cond server_ready)) in
  
  Domainslib.Task.await pool server;
  Domainslib.Task.await pool client1;
  Domainslib.Task.await pool client2
  
)