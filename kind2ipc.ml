exception BadMessage

type output_message = 

    (* Log message with level *)
    | Log of int * string

    (* Statistics *)
    | Stat of string

    (* Progress *)
    | Progress of int
        
  
  (* Message internal to the messaging system *)
type control_message = 
  (* Process is ready *)
  | Ready
  (* Request reply from process *)
  | Ping
  (* Request termination of process *)
  | Terminate
  (* Request resending of relay message *)
  | Resend of int

type message = 
    (* Output to user *)
    | OutputMessage of output_message
    (* Message internal to the messaging system *)
    | ControlMessage of control_message
  
let string_of_output_message (msg : output_message) =
  match msg with
  | Log (i,s) -> "LOG " ^ string_of_int i ^ " " ^ s
  | Stat s -> "STAT " ^ s
  | Progress i -> "PROGRESS " ^ string_of_int i

let string_of_control_message (msg : control_message) =
  match msg with
  | Ready -> "READY"
  | Ping -> "PING"
  | Terminate -> "TERMINATE"
  | Resend i -> "RESEND " ^ string_of_int i

let string_of_message (msg : message) =
  match msg with
  | OutputMessage m -> "OUTPUT;" ^ string_of_output_message m
  | ControlMessage m -> "CONTROL;" ^ string_of_control_message m

let output_message_of_string str = 
  let pre_string = String.sub str 0 (String.index str ' ') in
  let back_string = String.sub str (String.index str ' ' + 1) (String.length str) in
  match pre_string with
  | "LOG" -> 
    Log(int_of_string (String.sub back_string 0 (String.index back_string ' ')), String.sub back_string (String.index back_string ' ' + 1) (String.length back_string))
  | "STAT" -> Stat(back_string)
  | "PROGRESS" -> Progress(int_of_string back_string)
  | _ -> raise BadMessage

let message_of_string str =
  match String.sub str 0 (String.index str ';') with
  | "OUTPUT" -> OutputMessage(output_message_of_string (String.sub str (String.index str ';' + 1) (String.length str)))
  | "CONTROL" -> ControlMessage(control_message_of_string (String.sub str (String.index str ';' + 1) (String.length str)))
  | _ -> raise BadMessage

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

let send conn (msg : message) =
  let msg_string = string_of_message msg in
  send_frame conn (Cstruct.of_string msg_string)

let recv conn =
  let frame = recv_frame conn in
  let msg = Cstruct.to_string frame in
  let tag = String.sub msg 0 (String.index msg ';') in
  [tag; String.sub msg (String.index msg ';' + 1) (String.length msg)]