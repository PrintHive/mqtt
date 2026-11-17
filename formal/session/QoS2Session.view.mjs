export const title = "Your QoS 2 publish across dropped connections";

const publish = (v) => {
  switch (v.out) {
    case "none":
      return "Not published";
    case "Unacked":
      return v.written ? "Sent, waiting for PUBREC" : "Waiting for a connection";
    case "Released":
      return "PUBREL sent, waiting for PUBCOMP";
    case "done":
      return "Publish succeeded";
    case "dropped":
      return "Given up before sending";
  }
};

export const state = (v) => ({
  "Your publish": publish(v),
  Connection: v.up ? "Connected" : "Dropped",
  Broker: v.onward === 0 ? "Not delivered" : v.onward === 1 ? "Delivered once" : "Delivered twice"
});

export const group = (v) => (v.onward > 1 ? "bad" : v.out === "done" ? "good" : "working");

export const colors = { good: "teal", working: "blue", bad: "orange" };

export const step = (action, from, to) => {
  switch (action) {
    case "YouPublish":
      return "you publish at QoS 2";
    case "YouGiveUp":
      return "you interrupt the publish";
    case "ClientGetsPubrec":
      return "PUBREC arrives";
    case "ClientGetsPubcomp":
      return "PUBCOMP arrives";
    case "BrokerGetsPublish":
      return from.held ? "the broker gets a resent PUBLISH" : "the broker gets PUBLISH";
    case "BrokerGetsPubrel":
      return "the broker gets PUBREL";
    case "ConnectionDrops":
      return "the connection drops";
    case "Reconnects":
      return from.held && !to.held ? "the client reconnects [the broker forgot the session]" : "the client reconnects";
    // The broker's own message to you. These steps stay inside one chart state, so the chart draws none of them.
    case "ClientGetsPublish":
      return "the broker's message arrives";
    case "ClientGetsPubrel":
      return "the broker releases its message";
    case "BrokerSends":
      return "the broker sends you a message";
    case "BrokerGetsPubrec":
      return "the broker gets your PUBREC";
    case "BrokerGetsPubcomp":
      return "the broker gets your PUBCOMP";
    case "Done":
      return "nothing left to do";
  }

  return action;
};
