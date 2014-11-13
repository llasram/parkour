(ns parkour.counter
  (:refer-clojure :exclude [get])
  (:require [parkour (mapreduce :as mr)])
  (:import [clojure.lang Symbol Keyword]
           [org.apache.hadoop.mapreduce Counters CounterGroup Counter]))

(defn ^:private counter*
  [group cname]
  (let [group (or group "user")]
    (some-> mr/*context* (.getCounter group cname))))

(defn ^Counter counter
  "Get a job counter object for counter name `cname`.  The `cname` may provide
either both the counter group and base names, or only the base name for a
counter in the \"user\" group.  Returns `nil` if there is no active task
context.  The `cname` may be any of:
  - `nil`, yielding `nil`;
  - an existing counter;
  - a string, providing only the base name;
  - a bare symbol or keyword, providing only the base name;
  - a namespaced symbol or keyword; or
  - a sequence of two strings."
  [cname]
  (if (some? cname)
    (condp instance? cname
      Counter cname
      String (counter* "user" cname)
      Symbol (counter* (namespace cname) (name cname))
      Keyword (counter* (namespace cname) (name cname))
      :else (let [[group cname] cname] (counter* group cname)))))

(defn get
  "Current value of counter indicated by `cname`."
  [cname] (some-> cname counter .getValue))

(defn inc!
  "Increase counter indicated by `cname` by `value`, default 1."
  ([cname] (inc! cname 1))
  ([cname value] (some-> cname counter (.increment value))))

(defn set!
  "Set counter indicated by `cname` to `value`."
  [cname value] (some-> cname counter (.setValue value)))
