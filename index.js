var { objectSort } = require("object-projection");
var exp = {
  redisArgs: []
};
var redis = require("redis");
var jobsub;
var wosub;
var client = redis.createClient();

exp.work = (event, ...data) =>
  new Promise(async resolve => {
    let dataid = JSON.stringify(objectSort(data));
    let resolved;
    if (!jobsub) jobsub = redis.createClient(...exp.redisArgs);
    jobsub.subscribe(event + "_result_" + dataid);
    jobsub.on("message", (ev, d) => {
      if (ev != event + "_result_" + dataid) return;
      resolved = true;
      jobsub.unsubscribe(event + "_result_" + dataid);
      return resolve(JSON.parse(d));
    });
    //check processing and and queue list for existance
    let proccessing = await new Promise(res =>
      client.LRANGE(event + "::processing", 0, -1, (er, r) => res(r || []))
    );
    let queue = await new Promise(res =>
      client.LRANGE(event + "::queue", 0, -1, (er, r) => res(r || []))
    );

    if ([...proccessing, ...queue].includes(dataid) || resolved) return;
    client.LPUSH(event + "::queue", dataid);
    client.publish(event, "tick");
  });

var activeworkers = {};
exp.addWorker = (event, worker, thread = 1) => {
  if (!wosub) wosub = redis.createClient(...exp.redisArgs);
  wosub.subscribe(event);

  let listenerfunc = async () => {
    if (!activeworkers[event].thread) return;
    let input = await new Promise(res =>
      client.RPOPLPUSH(event + "::queue", event + "::processing", (er, r) =>
        res(r)
      )
    );
    if (input == null) return;
    activeworkers[event].thread--;
    let parsedinput;
    parsedinput = JSON.parse(input);
    let result;
    try {
      result = { data: await worker(...parsedinput), error: null };
    } catch (e) {
      result = { data: null, error: "can't do it" };
    }
    client.LREM(event + "::processing", 1, input);
    client.publish(event + "_result_" + input, JSON.stringify(result));
    activeworkers[event].thread++;
    client.publish(event, "tick");
  };

  activeworkers[event] = {
    thread,
    worker: listenerfunc
  };
  wosub.on("message", listenerfunc);
  client.publish(event, "tick");
};

exp.removeWorker = event => {
  if (!activeworkers[event]) return;
  wosub.removeListener("message", activeworkers[event].worker);
  delete activeworkers[event];
};

module.exports = exp;
