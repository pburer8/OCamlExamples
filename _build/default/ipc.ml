exception BadMessage
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

let send conn msg_type sender msg = 
  let zmsg = msg_type ^ ";" ^ sender ^ ";" ^ msg in

  send_frame conn (Cstruct.of_string zmsg)

let recv conn =
  let frame = recv_frame conn in
  let msg = Cstruct.to_string frame in
  let split_msg = String.split_on_char ';' msg in
  split_msg

module rec Publisher : sig
  type t = {
    mutex : Eio.Mutex.t;
    path : string;
    mutable subscribers : Subscriber.t list;
    stream : string Eio.Stream.t
  }
  
  val create : string -> t
  val path : t -> string
  val add_subscriber : t -> Subscriber.t -> unit
  val run_server : t -> Mutex.t -> Condition.t -> bool ref -> Eio_unix.Stdenv.base -> unit
end =
struct
  type t = {
    mutex : Eio.Mutex.t;
    path : string;
    mutable subscribers : Subscriber.t list;
    stream : string Eio.Stream.t
  }

  let create p = {
    mutex = Eio.Mutex.create (); path = p; subscribers = []; stream = Eio.Stream.create max_int
  }

  let path pub = pub.path

  let add_subscriber pub sub =
    Eio.Mutex.lock pub.mutex;
    pub.subscribers <- pub.subscribers @ [sub];
    Eio.Mutex.unlock pub.mutex

  let run_recv pub mu cond server_ready env = 
    let accept_loop server sw pub =
      while true do
        Eio.Net.accept_fork server ~sw ~on_error:raise
          (fun conn _addr ->
            let rec loop () =
              try
                let split_msg = recv conn in
                Eio.traceln "Server received: %S;%S;%S" (List.nth split_msg 0) (List.nth split_msg 1) (List.nth split_msg 2);
                Eio.Stream.add pub.stream (List.nth split_msg 2);
                loop ()
              with End_of_file -> ()
            in
            loop ())
      done
    in

    Eio.Switch.run @@ fun sw ->
      let net = Eio.Stdenv.net env in
      let server = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 (`Unix pub.path) in

      Mutex.lock mu;
      server_ready := true;
      Condition.broadcast cond;
      Mutex.unlock mu;

      accept_loop server sw pub

  let run_send pub env =
    Eio.Switch.run @@ fun sw ->
      let net = Eio.Stdenv.net env in
      let connections = List.map (fun sub -> Eio.Net.connect ~sw net (`Unix (Subscriber.path sub))) pub.subscribers in
      while true do 
        let msg = Eio.Stream.take pub.stream in
        
        List.iter (fun conn -> send conn "CONTROL" "SERVER" msg) connections;
      done

  let run_server sock mu cond server_ready env =
    Eio.Fiber.both
      (fun () -> run_recv sock mu cond server_ready env)
      (fun () -> run_send sock env)
end
and Subscriber : sig 
  type t = {
    mutex : Eio.Mutex.t;
    path : string;
    publisher : Publisher.t;
    mutable topics : string list
  }

  val create : string -> Publisher.t -> t
  val path : t -> string
  val subscribe : t -> string -> unit
  val run_client : t -> Mutex.t -> Condition.t -> bool ref -> Eio_unix.Stdenv.base -> 'a
end =
struct
  type t = {
    mutex : Eio.Mutex.t;
    path : string;
    publisher : Publisher.t;
    mutable topics : string list
  }

  let create p pub = {
    mutex = Eio.Mutex.create (); path = p; publisher = pub; topics = []
  }

  let path sub = sub.path

  let subscribe sub topic =
    Eio.Mutex.lock sub.mutex;
    sub.topics <- sub.topics @ [topic];
    Eio.Mutex.unlock sub.mutex

  
  let run_client sub mu cond server_ready env =
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in

    let listener = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 (`Unix sub.path) in

    Mutex.lock mu;
    while not !server_ready do Condition.wait cond mu done;
    Mutex.unlock mu;
    let out_conn = Eio.Net.connect ~sw net (`Unix (Publisher.path sub.publisher)) in
    Eio.traceln "Client: connected";
    send out_conn "CONTROL" sub.path "Hello!";  (* actual message to broadcast *)

    while true do
      Eio.Net.accept_fork listener ~sw ~on_error:raise
        (fun in_conn _addr ->
          try
            while true do
              let split_msg = recv in_conn in
              if sub.topics = [] then Eio.traceln "Received message unsubscribed";
              List.iter (fun topic -> 
                match split_msg with 
                | tag :: sender :: payload -> 
                  if Str.string_match (Str.regexp topic) tag 0
                    then Eio.traceln "Client %S: received %S;%S;%S" sub.path tag sender (String.concat "" payload)
                | _ -> raise BadMessage) sub.topics;
            done
          with End_of_file -> ())
    done
end

let create_new_subscriber id pub = 
  let path = "/tmp/eio_out" ^ (string_of_int id) ^ ".sock" in
  (try Unix.unlink path with Unix.Unix_error _ -> ());
  let sub = Subscriber.create path pub in
  Publisher.add_subscriber pub sub;
  sub

let () =
  let in_path = "/tmp/eio_in1.sock" in
  (* Clean up any leftover socket file *)
  (try Unix.unlink in_path with Unix.Unix_error _ -> ());


  (* Spin up two domains: one for the server, one for the client *)
  let pool = Domainslib.Task.setup_pool ~num_domains:4 () in

  let pub = Publisher.create in_path in
  let sub1 = create_new_subscriber 1 pub in
  let sub2 = create_new_subscriber 2 pub in
  let sub3 = create_new_subscriber 3 pub in

  Subscriber.subscribe sub1 "CONTROL";
  Subscriber.subscribe sub2 "CONTROL";
  Subscriber.subscribe sub3 "RELAY";

  
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
  let client3 = Domainslib.Task.async pool (fun () ->
    Eio_main.run (Subscriber.run_client sub3 mu cond server_ready)) in
  
  Domainslib.Task.await pool server;
  Domainslib.Task.await pool client1;
  Domainslib.Task.await pool client2;
  Domainslib.Task.await pool client3
  
)