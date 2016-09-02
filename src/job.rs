use serde_json::Value as JValue;
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize, Serializer, Deserializer, Error};
use chrono::{DateTime, UTC, NaiveDateTime};

#[derive(Debug, Clone)]
pub enum BoolOrUSize {
    Bool(bool),
    USize(usize),
}

#[derive(Debug, Clone)]
pub struct Job {
    pub class: String,
    pub jid: String,
    pub args: Vec<JValue>,
    pub created_at: DateTime<UTC>,
    pub enqueued_at: DateTime<UTC>,
    pub queue: String,
    pub retry: BoolOrUSize,

    pub extra: BTreeMap<String, JValue>,
    pub namespace: String,
}

impl Job {
    #[cfg_attr(feature="flame_it", flame)]
    fn with_namespace(&self, snippet: &str) -> String {
        if self.namespace == "" {
            snippet.into()
        } else {
            self.namespace.clone() + ":" + snippet
        }
    }
    #[cfg_attr(feature="flame_it", flame)]
    pub fn queue_name(&self) -> String {
        self.with_namespace(&("queue:".to_string() + &self.queue))
    }
}

impl Deserialize for Job {
    fn deserialize<D>(deserializer: &mut D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        let j = try!(JValue::deserialize(deserializer));
        if let Some(obj) = j.as_object() {
            let mut obj = obj.clone();
            Ok(Job {
                class: try!(obj.get("class")
                        .and_then(|v| v.as_str())
                        .ok_or(D::Error::custom("no member 'class'")))
                    .into(),
                args: try!(obj.get("args")
                        .and_then(|v| v.as_array())
                        .ok_or(D::Error::custom("no member 'args'")))
                    .clone(),
                queue: try!(obj.get("queue")
                        .and_then(|v| v.as_str())
                        .ok_or(D::Error::custom("no member 'queue'")))
                    .into(),
                jid: try!(obj.get("jid")
                        .and_then(|v| v.as_str())
                        .ok_or(D::Error::custom("no member 'jid'")))
                    .into(),
                retry: try!(obj.get("retry")
                    .and_then(|r| {
                        if r.is_u64() {
                            Some(BoolOrUSize::USize(r.as_u64().unwrap() as usize))
                        } else if r.is_boolean() {
                            Some(BoolOrUSize::Bool(r.as_bool().unwrap()))
                        } else {
                            None
                        }
                    })
                    .ok_or(D::Error::custom("no member 'retry'"))),
                created_at: try!(obj.get("created_at")
                        .and_then(|v| v.as_f64())
                        .map(|f| {
                            NaiveDateTime::from_timestamp(f as i64, ((f - f.floor()) * 1e9) as u32)
                        })
                        .map(|t| DateTime::from_utc(t, UTC))
                        .ok_or(D::Error::custom("no member 'created_at'")))
                    .into(),
                enqueued_at: try!(obj.get("enqueued_at")
                        .and_then(|v| v.as_f64())
                        .map(|f| {
                            NaiveDateTime::from_timestamp(f as i64, ((f - f.floor()) * 1e9) as u32)
                        })
                        .map(|t| DateTime::from_utc(t, UTC))
                        .ok_or(D::Error::custom("no member 'enqueued_at'")))
                    .into(),
                extra: {
                    obj.remove("class");
                    obj.remove("args");
                    obj.remove("queue");
                    obj.remove("jid");
                    obj.remove("retry");
                    obj.remove("created_at");
                    obj.remove("enqueued_at");
                    obj
                },
                namespace: "".into(),
            })
        } else {
            Err(D::Error::custom("not an object"))
        }
    }
}

impl Serialize for Job {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: Serializer
    {
        let mut state = try!(serializer.serialize_map(Some(7 + self.extra.len())));

        try!(serializer.serialize_map_key(&mut state, "class"));
        try!(serializer.serialize_map_value(&mut state, &self.class));

        try!(serializer.serialize_map_key(&mut state, "args"));
        try!(serializer.serialize_map_value(&mut state, &self.args));

        try!(serializer.serialize_map_key(&mut state, "queue"));
        try!(serializer.serialize_map_value(&mut state, &self.queue));

        try!(serializer.serialize_map_key(&mut state, "jid"));
        try!(serializer.serialize_map_value(&mut state, &self.jid));

        try!(serializer.serialize_map_key(&mut state, "created_at"));
        try!(serializer.serialize_map_value(&mut state,
                                            self.created_at.timestamp() as f64 +
                                            self.created_at.timestamp_subsec_nanos() as f64 / 1e9));

        try!(serializer.serialize_map_key(&mut state, "enqueued_at"));
        try!(serializer.serialize_map_value(&mut state,
                                            self.enqueued_at.timestamp() as f64 +
                                            self.enqueued_at.timestamp_subsec_nanos() as f64 /
                                            1e9));


        match self.retry {
            BoolOrUSize::Bool(x) => {
                try!(serializer.serialize_map_key(&mut state, "retry"));
                try!(serializer.serialize_map_value(&mut state, x));
            }
            BoolOrUSize::USize(x) => {
                try!(serializer.serialize_map_key(&mut state, "retry"));
                try!(serializer.serialize_map_value(&mut state, x));
            }
        };

        for (k, v) in &self.extra {
            try!(serializer.serialize_map_key(&mut state, k));
            try!(serializer.serialize_map_value(&mut state, v));
        }

        serializer.serialize_map_end(state)

    }
}

pub struct Retry {
    retry_count: usize,
    error_message: String,
    error_class: String,
    error_backtrace: Vec<String>,
    failed_at: f64,
    retried_at: f64,
}