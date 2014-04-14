(ns parkour.io.seqf
  (:require [parkour (conf :as conf) (fs :as fs)]
            [parkour.io (dseq :as dseq) (dsink :as dsink)])
  (:import [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.input SequenceFileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output SequenceFileOutputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]))

(defn dseq
  "Distributed sequence for reading from Hadoop sequence files at `paths`."
  [& paths]
  (dseq/dseq
   (fn [^Job job]
     (.setInputFormatClass job SequenceFileInputFormat)
     (FileInputFormat/setInputPaths job (fs/path-array paths)))))

(defn dsink
  "Distributed sink writing tuples of classes `ckey` and `cval` to `path`."
  [[ckey cval] path]
  (let [cval (if-not (nil? cval) cval NullWritable)]
    (dsink/dsink
     (dseq path)
     (fn [^Job job]
       (doto job
         (.setOutputFormatClass SequenceFileOutputFormat)
         (.setOutputKeyClass ckey)
         (.setOutputValueClass cval)
         (FileOutputFormat/setOutputPath (fs/path path)))))))
