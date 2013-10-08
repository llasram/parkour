(ns parkour.io.text
  (:require [parkour (conf :as conf) (fs :as fs) (graph :as pg)])
  (:import [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.input TextInputFormat]
           [org.apache.hadoop.mapreduce.lib.output TextOutputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]))

(defn dseq
  "Distributed sequence of input text file lines.  Tuples consist
of (file offset, text line)."
  [& paths]
  (pg/dseq
   (fn [^Job job]
     (.setInputFormatClass job TextInputFormat)
     (doseq [path paths]
       (FileInputFormat/addInputPath job (fs/path path))))))

(defn dsink
  "Distributed sink for line-delimited text output.  Produces one line
per tuple containing TAB-separated results of invoking `.toString`
method of tuple members."
  [path]
  (pg/dsink
   (dseq path)
   (fn [^Job job]
     (doto job
       (.setOutputFormatClass TextOutputFormat)
       (.setOutputKeyClass Object)
       (.setOutputValueClass Object)
       (FileOutputFormat/setOutputPath (fs/path path))))))