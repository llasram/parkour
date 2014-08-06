(ns parkour.io.dexpr-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (reducers :as pr)
             ,       (mapreduce :as mr) (graph :as pg) (toolbox :as ptb)]
            [parkour.io (text :as text) (seqf :as seqf)]
            [parkour.io.dexpr :refer [dexpr]]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn rcomp
  [& fs] (apply comp (reverse fs)))

(deftest test-dexprs
  (th/with-config
    (let [conf (conf/ig)
          inpath (-> "word-count-input.txt" io/resource io/file str)
          mf (dexpr (rcomp (r/mapcat #(str/split % #"\s+"))
                           (r/map #(-> [% 1]))))
          rf (dexpr ^{::mr/source-as :keyvalgroups}
                    (partial ptb/keyvalgroups-r +))
          wc (-> (pg/input (text/dseq inpath))
                 (pg/map mf)
                 (pg/partition [Text LongWritable])
                 (pg/reduce rf)
                 (pg/output (seqf/dsink [Text LongWritable]))
                 (pg/fexecute conf `word-count)
                 (->> (into {})))]
      (is (= {"apple" 3, "banana" 2, "carrot" 1} wc)))))
