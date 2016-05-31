(ns sreduce.util
  (:require
   #?@(:clj  [[clojure.core.async :as async :refer [<! >! timeout chan promise-chan pipe onto-chan go go-loop alts!]]]
       :cljs [[cljs.core.async :as async :refer [<! >! timeout chan promise-chan pipe onto-chan]]]))
  #?(:cljs (:require-macros
            [cljs.core.async.macros :as m :refer [go go-loop]])))


(defn launch-reductions [c-redn f l ps]
  (let [pairs (take-while (fn [[a b]] (and a b))
                          (partition 2 (map deref ps)))]
    (map (fn [[a b]]
           (let [v (volatile! nil)]
             (go (>! c-redn [(inc l) (<! (f a b)) v]))
             v)) pairs)))

;;(defn wrapv [c] (pipe c (chan 1 (map (fn [x] [0 x nil])))))

(defn wrapv [] (chan 1 (map (fn [x] [0 x nil]))))

(defn pretty-state [{:keys [queues np n]}]
  (let [queues  (map (fn [l] (let [psd (map deref  (or (queues l) []))]
                             [(take-while identity (take 2 psd)) (count psd) (count (filter not psd))]
                             ))
                    (range 10))]
    {:n n :np np :queues queues}))

(defn assoc-reduce [f c-in & {:keys [np-max debug n] :or {np-max 5 debug false}}]
  (let [c-result (promise-chan)
        c-redn    (chan np-max)]
    (go-loop [{:keys [c-in queues np] :as state} {:c-in (pipe c-in (wrapv)) :queues {} :np 0 :i 0 :n n}]
      ;;(prn (pretty-state state))
      (if debug (>! debug (pretty-state state)))
      (if-let [cs (seq (filter identity (list (if (pos? np) c-redn) (if (< np np-max) c-in))))]
        (let [[[l res v]  c]  (alts! cs)]
          (if-not l
            (recur (assoc state :c-in nil))
            (let [q (if v
                       (do (vreset! v res) (queues l))
                       (concat (queues 0) [(volatile! res)]))
                  vs  (launch-reductions c-redn f l q)
                  nr  (count vs)
                  q   (drop (* 2 nr) q )
                  np  (cond-> (+ np nr) (pos? l) dec)
                  l2  (inc l)
                  q2 (concat (queues l2) vs)
                  n   (cond-> (:n state) (zero? l) (dec))]
              (recur (assoc state :n n :np np :queues (assoc queues l q l2 q2))))))
        (let [reds (->> (seq queues)
                        (sort-by first)
                        (map second)
                        (map first)
                        (filter identity)
                        (map deref)
                        reverse
                        )]
          ;(if debug (prn "Reducing reductions" reds))
          (if (<= (count reds) 1)
            (do
              ;(prn "Returning" (first reds))
              (when debug (async/close! debug))
              (>! c-result (first reds)))
            (let [c-in (wrapv)]
              (onto-chan c-in reds)
              (recur {:n (count reds) :c-in c-in :queues {} :np 0}))))))
    c-result))


(defn delay-spool [as t do-rand]
  (let [c (chan)]
    (go-loop [[a & as] as]
      (if a
        (do (>! c a)
            (<! (timeout (if do-rand (rand-int t) t)))
            (recur as))
        (async/close! c)))
    c))

(defn plusso [t do-rand]
  (fn [a b] (go (<! (timeout (if do-rand (rand-int t) t))) (+ a b))))
