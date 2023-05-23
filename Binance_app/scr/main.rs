use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Error;
use tokio::net::{TcpListener, TcpStream};
use common_math::rounding::ceil;
use std::sync::{Arc, Mutex};
use std::{thread, time};

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

// This one if part of out_messages struct
#[repr(C)] #[derive(Debug, Serialize)]
pub struct data_for_out_message {
    t: u64,
    o: f64,
    c: f64,
    h: f64,
    l: f64,
}

// This struct is for incomming messages
#[derive(Debug, Deserialize)]
pub struct setup_for_stream {
    time_interval: String,
    currencies: String,
}

// This const have name for our out_message stream field. Plus, it has streams, we need to listen to
// and our out stream adress

const ADRESS: &str = "127.0.0.1:3030";

// Our main fn. Has nothing, except start button if we got some incoming connection - we start.
#[tokio::main]
async fn main() {
    loop {
        let listener = TcpListener::bind(ADRESS).await.expect("1");
        let (stream, _) = listener.accept().await.expect("2");
        let ws_stream = tokio_tungstenite::accept_async(stream).await.expect("3");
        // Each incomming client got his own corutine
        tokio::task::spawn ( async {
        if let Err(e) = run(ws_stream).await {
            println!("Error: {}", e);
        }
    });

}
}

// Our working fn. Start every time we got incoming connection.
async fn run(ws_stream: WebSocketStream<TcpStream>) -> Result<(), Box<dyn std::error::Error>> {


    // This are mutexes to communicate throught threads
    let first_is_negative = Arc::new(Mutex::new(false));
    let signs = Arc::new(Mutex::new(Vec::new()));
    let break_sign = Arc::new(Mutex::new(false));

    // This is vec for currencies we got from client request
    let mut currencies = Vec::new();

    // This is incomming stream split
    let (mut write, mut read) = ws_stream.split();
    println!("we got connection");

    // We get message from client
    let setup_info = read.next().await;
    let mut time_interval = "".to_string();
    let mut stream_name = "".to_string();

    // This is setup for our stream, recieved from the client
    match setup_info {
        Some(info) => {
            match info {
                Ok(info) => {
                    let setup = info.into_text().unwrap();
                    match setup_message_making(&setup) {
                        Ok(T) => {
                            time_interval = T.time_interval;
                            stream_name = T.currencies.clone();
                            currencies = task_making(Arc::clone(&signs), Arc::clone(&first_is_negative), T.currencies);

                        },
                        Err(E) => println!("error {:?}", E)
                    }
                }
                Err(E) => println!("we got error {:?}", E)
            }

        }
        None => println!("no setup message")
    }

    let current_prices = Arc::new(Mutex::new(vec![1.0; currencies.len()*5]));

    // Here we start our listening streams. Our last thread start only in the end of this code
    // So, it tells us about disconnection
    for mut i in 0..=currencies.len() {

        if i == currencies.len(){
            tokio::task::spawn ( async move{
            println!("we got disconnection");
            });
        } else {
            let wss_url = format!("wss://stream.binance.com:9443/ws/{}@kline_{}", currencies[i], &time_interval);

            let prices = Arc::clone(&current_prices);
            let sign = Arc::clone(&break_sign);
            tokio::task::spawn(async move {
                println!("we are here {}", i);
                if let Err(e) = connection_to_binance(&wss_url, i, prices, sign).await {
                    println!("Error: {}", e);
                }
            });
        }

    }


    // Here we send messages
    loop{
        // We send messages every _ seconds.
        thread::sleep(time::Duration::from_secs(5));

        // This ar clones of our mutexes
        let price = Arc::clone(&current_prices);
        let signes = Arc::clone(&signs);
        let first_negative = Arc::clone(&first_is_negative);
        let break_sign1 = Arc::clone(&break_sign);

        //This is fn, that produce our messages variables
        let (same_time, t, o, c, h, l) = message_produce(price, signes, first_negative);

        // This is our message
        let message1 = out_message {
            stream: stream_name.to_string(),
            data: data_for_out_message {
                t: t,
                o: ceil(o, 2),
                c: ceil(c, 2),
                h: ceil(h, 2),
                l: ceil(l, 2)
            }
        };




        // We check out, if we have the same time in all currency info
        if same_time {
            // We serialize message and send it
            let message = serde_json::to_string(&message1).expect("4");

            // If our incoming connection goes offline - we break
            if let Err(_) = write.send(Message::text(message.to_string())).await {
                let mut break_sign2 = break_sign1.lock().unwrap();
                *break_sign2 = true;
                break;
        };
        }





    }

    Ok(())
}

// This fn give us variables for our message
fn message_produce (prices: Arc<Mutex<Vec<f64>>>, signs: Arc<Mutex<Vec<char>>>,
                    first_is_negative: Arc<Mutex<bool>>) -> (bool, u64, f64, f64, f64, f64) {
    // It is true, if all the times in our info match
    let mut same_time = true;
    let (mut t, mut o, mut c, mut h, mut l) = (0, 0.0, 0.0, 0.0, 0.0);

    // This are prices of our coins
    let prices1 = prices.lock().unwrap();
    // If the first char is '-' - this one will be true
    let first_negative = first_is_negative.lock().unwrap();
    if *first_negative{
        (t, o, c, h, l) = (prices1[0] as u64, -prices1[1], -prices1[2], -prices1[3], -prices1[4]);
    } else {
        (t, o, c, h, l) = (prices1[0] as u64, prices1[1], prices1[2], prices1[3], prices1[4]);
    }


    // This are signes we use
    let signes = signs.lock().unwrap();
    let number = prices1.len()/5;
    for i in 1..number {
        if prices1[i*5+0] as u64 != t {
            same_time = false
        }
        if signes[i-1] == '+' {
            o+=prices1[i*5+1];
            c+=prices1[i*5+2];
            h+=prices1[i*5+3];
            l+=prices1[i*5+4];
        } else if signes[i-1] == '-' {
            o-=prices1[i*5+1];
            c-=prices1[i*5+2];
            h-=prices1[i*5+3];
            l-=prices1[i*5+4];
        } else if signes[i-1] == '*' {
            o *= prices1[i * 5 + 1];
            c *= prices1[i * 5 + 2];
            h *= prices1[i * 5 + 3];
            l *= prices1[i * 5 + 4];
        } else if signes[i-1] == '/' {
            o /= prices1[i * 5 + 1];
            c /= prices1[i * 5 + 2];
            h /= prices1[i * 5 + 3];
            l /= prices1[i * 5 + 4];
        }
    }


    return (same_time, t, o, c, h, l)

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
        stream: "".to_string(),
        data: data_for_out_message {
            t: t,
            o: o,
            c: c,
            h: h,
            l: l
        }
    }
}

fn setup_message_making (json_str: &str) -> Result<setup_for_stream, Error> {
    serde_json::from_str(json_str)
}

// This fn make task for our stream
fn task_making (signs: Arc<Mutex<Vec<char>>>, first_is_negative: Arc<Mutex<bool>>, message: String) -> Vec<String> {
    let mut answer: Vec<String> = Vec::new();
    let mut answer_string = "".to_string();
    // We get chars from our string. There are words, separated by signes
    for i in message.chars() {
        // If first char is '-' - we check it in our first_is_negative variable
        if answer.len() == 0 && i == '-' {
            let mut first_is_negative1 = first_is_negative.lock().unwrap();
            *first_is_negative1 = true;
        // Then, we push signs to the signs vec if we met one
        } else if i == '+' || i == '-' || i == '/' || i == '*' {
            let mut signs1 = signs.lock().unwrap();
            signs1.push(i);
            answer.push(answer_string);
            answer_string = "".to_string();

        // If it is not signs - we push them to string. That will be pushed to returned Vec when we
        // Got a word
        } else {
            answer_string.push(i)
        }
    }
    answer.push(answer_string);
    return answer
}

// This fn give us incomming streams from binance
async fn connection_to_binance (url: &str, number: usize, prices: Arc<Mutex<Vec<f64>>>, break_sign: Arc<Mutex<bool>>)
    -> Result<(), Box<dyn std::error::Error>>{
    println!("we are there {}", number);
    let mut message: out_message;
    // We connect to stream and split it. We are interested in read part
    let (stream, _) = connect_async(url).await?;
    let (_, mut read) = stream.split();
    loop {
        // We listen to info
        if let Some(msg) = read.next().await {
             match msg {
                 Ok(msg) =>  {

                     message = message_check_addition(msg);

                     // We write all info to our prices vec
                     let mut prices1 = prices.lock().unwrap();
                     prices1[number*5+0] = message.data.t as f64;
                     prices1[number*5+1] = message.data.o;
                     prices1[number*5+2] = message.data.c;
                     prices1[number*5+3] = message.data.h;
                     prices1[number*5+4] = message.data.l;
                 },
                 Err(E) => println!("Its error: {:?}", E)
         }
        }
        // If our incomming stream go down - we break
        let break_sign1 = break_sign.lock().unwrap();
        if *break_sign1 {
            break
        }

    }

    Ok(())
}