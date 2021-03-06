(ns parkour.mapreduce
  (:refer-clojure :exclude [keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [clojure.reflect :as reflect]
            [parkour (conf :as conf) (wrapper :as w) (reducers :as pr)]
            [parkour.mapreduce (source :as src) (sink :as snk)]
            [parkour.util :refer [returning ignore-errors]])
  (:import [java.io Writer]
           [clojure.lang Var]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce TaskAttemptContext TaskAttemptID]
           [org.apache.hadoop.mapreduce Counters CounterGroup Counter]))

(defn keys
  "Produce keys only from the tuples in `context`."
  [context] (src/reducer src/next-keyval src/key context))

(defn vals
  "Produce values only from the tuples in `context`."
  [context] (src/reducer src/next-keyval src/val context))

(defn keyvals
  "Produce pairs of keys and values from the tuples in `context`."
  [context] (src/reducer src/next-keyval src/keyval context))

(defn keygroups
  "Produce distinct keys from the tuples in `context`."
  [context] (src/reducer src/next-key src/key context))

(defn valgroups
  "Produce sequences of values associated with distinct grouping keys from the
tuples in `context`."
  [context] (src/reducer src/next-key src/vals context))

(defn keyvalgroups
  "Produce pairs of distinct group keys and associated sequences of values from
the tuples in `context`."
  [context] (src/reducer src/next-key src/keyvals context))

(defn keykeyvalgroups
  "Produce pairs of distinct grouping keys and associated sequences of specific
keys and values from the tuples in `context`."
  [context] (src/reducer src/next-key src/keykeyvals context))

(defn keykeygroups
  "Produce pairs of distinct grouping keys and associated sequences of specific
keys from the tuples in `context`."
  [context] (src/reducer src/next-key src/keykeys context))

(defn keysgroups
  "Produce sequences of specific keys associated with distinct grouping keys
from the tuples in `context`."
[context] (src/reducer src/next-key src/keys context))

(defn wrap-sink
  "Return new tuple sink which wraps keys and values as the types `ckey` and
`cval` respectively, which should be compatible with the key and value type of
`sink`.  Where they are not compatible, the type of the `sink` will be used
instead.  Returns a new tuple sink which wraps any sunk keys and values which
are not already of the correct type then sinks them to `sink`."
  ([sink] (snk/wrap-sink sink))
  ([ckey cval sink] (snk/wrap-sink ckey cval sink)))

(defn sink-as
  "Annotate `coll` as containing values to sink as `kind`.  The `kind` may
either be a sinking function of two arguments (a sink and a collection) or a
keyword indicating a built-in sinking function.  Supported keywords are `:none`,
`:keys`, `:vals`, and `:keyvals`."
  [kind coll] (vary-meta coll assoc ::snk/sink-as kind))

(defn sink
  "Emit all tuples from `coll` to `sink`."
  [sink coll] ((snk/sink-fn coll) sink coll))

(def ^:private job-factory-method?
  "True iff the `Job` class has a static factory method."
  (->> Job reflect/type-reflect :members (some #(= 'getInstance (:name %)))))

(defmacro ^:private make-job
  "Macro to create a new `Job` instance, using Hadoop version appropriate
mechanism."
  [& args] `(~(if job-factory-method? `Job/getInstance `Job.) ~@args))

(defn job
  "Return new Hadoop `Job` instance, optionally initialized with configuration
`conf`."
  {:tag `Job}
  ([] (make-job (conf/ig)))
  ([conf] (make-job (conf/clone conf))))

(defmethod print-method Job
  [job ^Writer w]
  (.write w "#hadoop.mapreduce/job ")
  (print-method (conf/diff job) w))

(defn mapper!
  "Allocate and return a new Parkour mapper class for `conf` as invoking `var`.
The `var` will be called during task-setup with the job `Configuration` and any
provided `args` (which must be EDN-serializable).  The `var` should return a
function of two arguments, which will be invoked with the task context and a
reducible collection of the `unwrap`ed input tuples.  It should return a
reducible collection of the output tuples, which will be automatically wrapped
to match the job-configured map ouput types.

If `var` has a truthy value for the `:parkour.mapreduce/raw` metadata key, then
Parkour will invoke the `var`-returned function with only the task context, and
will ignore any return value."
  [conf var & args]
  (assert (instance? Var var))
  (let [i (conf/get-int conf "parkour.mapper.next" 0)]
    (conf/assoc! conf
      "parkour.mapper.next" (inc i)
      (format "parkour.mapper.%d.var" i) (pr-str var)
      (format "parkour.mapper.%d.args" i) (pr-str args))
    (Class/forName (format "parkour.hadoop.Mappers$_%d" i))))

(defn ^:private reducer!*
  [step conf var & args]
  (assert (instance? Var var))
  (let [i (conf/get-int conf "parkour.reducer.next" 0)]
    (conf/assoc! conf
      "parkour.reducer.next" (inc i)
      (format "parkour.reducer.%d.step" i) (name step)
      (format "parkour.reducer.%d.var" i) (pr-str var)
      (format "parkour.reducer.%d.args" i) (pr-str args))
    (Class/forName (format "parkour.hadoop.Reducers$_%d" i))))

(defn reducer!
  "Allocate and return a new Parkour reducer class for `conf` as invoking `var`.
The `var` will be called during task-setup with the job `Configuration` and any
provided `args` (which must be EDN-serializable).  The `var` should return a
function of two arguments, which will be invoked with the task context and a
reducible collection of the `unwrap`ed input tuples.  It should return a
reducible collection of the output tuples, which will be automatically wrapped
to match the job-configured map ouput types.

If `var` has a truthy value for the `:parkour.mapreduce/raw` metadata key, then
Parkour will invoke the `var`-returned function with only the task context, and
will ignore any return value."
  [conf var & args]
  (apply reducer!* :reduce conf var args))

(defn combiner!
  "As per `reducer!`, but allocate and configure for the Hadoop combine step,
which may impact e.g. output types."
  [conf var & args]
  (apply reducer!* :combine conf var args))

(defn partitioner!
  "Allocate and return a new Parkour partitioner class for `conf` as invoking
`var`.  The `var` will be called during task-setup with the job `Configuration`
and any provided `args` (which must be EDN-serializable).  The `var` should
return a function of three arguments: an `unwrap`ed map-output key, an
`unwrap`ed map-output value, and an integral reduce-task count.  That function
will called for each map-output tuple, must return an integral value mod the
reduce-task count, and should be primitive-hinted as `OOLL`.

If `var` has a truthy value for the `:parkour.mapreduce/raw` metadata key, then
Parkour will invoke the `var`-returned partitioner function with the raw (not
unwrapped) tuple key and value objects."
  [conf var & args]
  (assert (instance? Var var))
  (conf/assoc! conf
    "parkour.partitioner.var" (pr-str var)
    "parkour.partitioner.args" (pr-str args))
  parkour.hadoop.Partitioner)

(defn set-mapper
  "Set the mapper class for `job` to `cls`."
  [^Job job cls] (.setMapperClass job cls))

(defn set-combiner
  "Set the combiner class for `job` to `cls`."
  [^Job job cls] (.setCombinerClass job cls))

(defn set-reducer
  "Set the reducer class for `job` to `cls`."
  [^Job job cls] (.setReducerClass job cls))

(defn set-partitioner
  "Set the partitioner class for `job` to `cls`."
  [^Job job cls] (.setPartitionerClass job cls))

(let [cfn (fn [c] (Class/forName (str "org.apache.hadoop.mapreduce." c)))
      klass (or (ignore-errors (cfn "task.TaskAttemptContextImpl"))
                (ignore-errors (cfn "TaskAttemptContext")))
      cname (-> ^Class klass .getName symbol)]
  (defmacro ^:private tac*
    [& args] `(new ~cname ~@args)))

(defprotocol ^:private GetTaskAttemptID
  "Protocol for extracting a task attempt ID."
  (^:private -taid [x] "Task attempt ID for `x`."))

(extend-protocol GetTaskAttemptID
  TaskAttemptContext (-taid [tac] (.getTaskAttemptID tac))
  TaskAttemptID (-taid [taid] taid))

(defn ^:private taid
  "Return `TaskAttemptID` for `x` when provided, or a new `TaskAttemptID`."
  ([] (TaskAttemptID. "parkour" 0 false 0 0))
  ([x] (-taid x)))

(defn tac
  "Return a new TaskAttemptContext instance using provided configuration `conf`
and task attempt ID `taid`."
  {:tag `TaskAttemptContext}
  ([conf] (tac conf (taid)))
  ([conf id] (tac* (conf/ig conf) (taid id))))

(defn counters-map
  "Translate job `counters` into nested Clojure map of strings to counts."
  [counters]
  (if (and counters (not (instance? Counters counters)))
    (->> counters meta ::counters counters-map)
    (let [c-entry (juxt #(.getName ^Counter %) #(.getValue ^Counter %))
          group (fn [g] (->> g (r/map c-entry) (into {})))
          g-entry (juxt #(.getName ^CounterGroup %) group)]
      (->> counters (r/map g-entry) (into {})))))
