(ns parkour.io.dexpr
  (:require [parkour.cser :as cser])
  (:import [java.io Writer]
           [clojure.lang IDeref IFn IObj]))

(defmacro ^:private deftype-ifn
  "Like `deftype`, but implement `IFn` delegating to `dname`."
  [name fields dname & specs]
  (let [dname (vary-meta dname assoc :tag `IFn)]
    `(deftype ~name ~fields
       IFn
       ~@(map (fn [n]
                (let [args (map #(symbol (str "arg" %)) (range n))]
                  `(~'invoke [~'_ ~@args] (. ~dname ~'invoke ~@args))))
           (range 0 22))
       (~'applyTo [~'_ ~'args] (. ~dname ~'applyTo ~'args))
       ~@specs)))

(deftype-ifn DExpr [meta ns form value]
  #_IFn
  value

  Object
  (toString [_] (str "#=" (pr-str form)))
  (hashCode [_] (.hashCode ^Object value))
  (equals [_ o]
    (and (instance? DExpr o)
         (.equals value (.-value ^DExpr o))))

  IObj
  (meta [_] meta)
  (withMeta [_ meta] (DExpr. meta ns form value))

  IDeref
  (deref [_] value))

(defmethod print-method DExpr
  [^DExpr x ^Writer w]
  (.write w "#parkour/dexpr " )
  (.write w (pr-str [(.-meta x) (.-ns x) (.-form x)])))

(defn ^:private dexpr-reader
  {:tag `DExpr}
  [[meta ns form]]
  (let [value (binding [*ns* *ns*] (require ns) (in-ns ns) (eval form))]
    (->DExpr meta ns form value)))

(defmacro dexpr
  "Distributed-evaluation expression for form `body`."
  [body]
  (let [meta (-> body meta (dissoc :column :line))
        ns `(quote ~(ns-name *ns*))
        form `(quote ~body)
        used? (->> body flatten (filter symbol?) set)
        locals (filter used? (keys &env))
        form (if (empty? locals)
               form
               ``(let ~[~@(mapcat #(-> [`(quote ~%) %]) locals)]
                   ~~form))]
    `(->DExpr ~meta ~ns ~form ~body)))
