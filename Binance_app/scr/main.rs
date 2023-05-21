use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Error;
use tokio::net::TcpListener;
use common_math::rounding::ceil;
use std::ops;

// This are 2 struct for our incomming messages Deserialize
#[derive(Debug, Deserialize)]
pub struct KlineData {
    t: u64,
    T: u64,
    s: String,
    i: String,
    f: u64,
    L: u64,
    o: String,
    c: String,
    h: String,
    l: String,
    v: String,
    n: u32,
    x: bool,
    q: String,
    V: String,
    Q: String,
    B: String,
}

#[derive(Debug, Deserialize)]
pub struct KlineMessage {
    e: String,
    E: u64,
    s: String,
    k: KlineData,
}

// This is struct for our outcomming messages
#[repr(C)] #[derive(Debug, Serialize)]
pub struct out_message {
    stream: String,
    data: data_for_out_message,
}

// This is for we can ust += with out_messages struct
impl ops::AddAssign for out_message {


    fn add_assign (&mut self, another: Self){
        *self = Self {
            stream: another.stream,
            data: data_for_out_message{
                t: another.data.t,
                o: ceil(self.data.o + another.data.o, 2),
                c: ceil(self.data.c + another.data.c, 2),
                h: ceil(self.data.h + another.data.h, 2),
                l: ceil(self.data.l + another.data.l, 2)
            },
        }
    }

}

// This one if part of out_messages struct
#[repr(C)] #[derive(Debug, Serialize)]
pub struct data_for_out_message {
    t: u64,
    o: f64,
    c: f64,
    h: f64,
    l: f64,
}

// This const have name for our out_message stream field. Plus, it has streams, we need to listen to
// and our out stream adress
const OUT_STREAM_NAME: &str = "btcusdt+ethusd@1m";
const BINANCE_BTCSTREAM: &str = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m";
const BINANCE_ETHSTREAM: &str = "wss://stream.binance.com:9443/ws/ethusdt@kline_1m";
const ADRESS: &str = "127.0.0.1:3030";

// Our main fn. Has nothing, except start button if we got some incoming connection - we start.
#[tokio::main]
async fn main() {
    loop {
        let listener = TcpListener::bind(ADRESS).await.expect("1");
        if let Err(e) = run(listener).await {
            println!("Error: {}", e);
        }
    }
}

// Our working fn. Start every time we got incoming connection.
async fn run(listener: TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    // Here we take and split our incoming message
    let (stream, _) = listener.accept().await.expect("2");
    let ws_stream = tokio_tungstenite::accept_async(stream).await.expect("3");
    let (mut write, _) = ws_stream.split();
    println!("we got connection");

    // We got 2 different out connection (one for btc, one for eth)
    let (btc_stream, _) = connect_async(BINANCE_BTCSTREAM).await?;
    let (eth_stream, _) = connect_async(BINANCE_ETHSTREAM).await?;


    // We split both our connection
    let (_, mut read_btc) = btc_stream.split();
    let (_, mut read_eth) = eth_stream.split();


    // We start our infinite loop
    loop{
        //This is our message. It is empty for now
        let mut message1 = out_message {
            stream: OUT_STREAM_NAME.to_string(),
            data: data_for_out_message {
                t: 0,
                o: 0.0,
                c: 0.0,
                h: 0.0,
                l: 0.0
            }
        };
        let mut same_time = true;
        // Here we got our btc info and add it to our message
        if let Some(msg) = read_btc.next().await {
             match msg {
                 Ok(msg) =>  {
                     message1 += message_check_addition(msg);
                 },
                 Err(E) => println!("Its error: {:?}", E)
         }
        }
        // Here we got our eth info and add it to our message
        if let Some(msg) = read_eth.next().await {
            match msg {
                 Ok(msg) =>  {
                     let message2 = message_check_addition(msg);
                     if message1.data.t != message2.data.t {
                         same_time = false;
                     }
                     message1 += message2;
                 },
                 Err(E) => println!("Its error: {:?}", E)
         }
        }
        // We check out, if bot eth and btc messages have the same start time
        if same_time {
            // We serialize message and send it
            let message = serde_json::to_string(&message1).expect("4");

            // If our incoming connection goes offline - we break
            if let Err(_) = write.send(Message::text(message.to_string())).await {
            break;
        };
        }





    }
    println!("we got disconnection");
    Ok(())
}

// This fn help us to deserialize inc messages
fn parse_kline_message(json_str: &str) -> Result<KlineMessage, Error> {
    serde_json::from_str(json_str)
}

// This fn produce out_message struct from messages from Binance
fn message_check_addition (msg: Message) -> out_message {
    let (mut t, mut o, mut c, mut h, mut l) = (0,0.0,0.0,0.0,0.0);

    // We take info from messages
    let message = msg.into_text().expect("5");
    match parse_kline_message(&message) {
        Ok(kline_message) => {
            t = kline_message.k.t;
            o = kline_message.k.o.parse().expect("6");
            c = kline_message.k.c.parse().expect("7");
            h = kline_message.k.h.parse().expect("8");
            l = kline_message.k.l.parse().expect("9");

        }
        Err(error) => eprintln!("Ошибка при разборе сообщения: {:?}", error),
    }

    // And make out_message struct
    out_message {
        stream: OUT_STREAM_NAME.to_string(),
        data: data_for_out_message {
            t: t,
            o: o,
            c: c,
            h: h,
            l: l
        }
    }
}