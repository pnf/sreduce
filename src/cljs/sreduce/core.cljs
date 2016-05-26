(ns sreduce.core
  (:require-macros [cljs.core.async.macros :as m :refer [go go-loop]])
  (:require [reagent.core :as r :refer [atom]]
            [reagent.session :as session]
            [timothypratley.reanimated.core :as anim]
            [secretary.core :as secretary :include-macros true]
            [accountant.core :as accountant]
            [cljs.core.async :as async :refer [<! >! timeout chan promise-chan pipe onto-chan]]))

(defonce timer (r/atom (js/Date.)))
(defonce timer2  (r/atom (js/Date.)))

(defonce time-color (r/atom "#f34"))

(defonce time-chan (chan (async/dropping-buffer 1)))

(defonce time-updater (js/setInterval
                       #(do
                          (go (>! time-chan (js/Date.)))
                          (reset! timer (js/Date.)))
                       100))

(defonce time-receiver (go-loop []
                         (reset! timer2 (<! time-chan))
                         (recur)
                         ))

(defn clock []
  (let [time-str  (-> @timer .toTimeString (clojure.string/split " ") first)
        time-str2 (-> @timer2 .toTimeString (clojure.string/split " ") first)
        time-str (str time-str "---" time-str2)]
    [:div.example-clock
     {:style {:color @time-color}}
     time-str]))

(defn plusso [t do-rand]
  (fn [a b] (go (<! (timeout (if do-rand (rand-int t) t))) (+ a b))))

(defn delay-spool [as t do-rand]
  (let [c (chan)]
    (go-loop [[a & as] as]
      (if a
        (do (>! c a)
            (<! (timeout (if do-rand (rand-int t) t)))
            (recur as))
        (async/close! c)))
    c))

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

(defn assoc-reduce3 [f c-in & {:keys [np-max debug n] :or {np-max 5 debug false}}]
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


;; -------------------------
;; Views

(defn home-page []
  [:div [:h2 "Welcome to sreduce"]
   [:div [:a {:href "/about"} "go to about page"]]])

(defonce result (r/atom "Waiting"))

(defonce progress (r/atom "progress"))

(defn start-reduce [n do-rand t-in t-red np-max]
  (prn n t-in t-red np-max)
  (let [dc (chan 10)
        c (assoc-reduce3 (plusso t-red do-rand) (delay-spool (range n) t-in do-rand)  :debug dc :np-max np-max :n n)]
    (go
      (reset! result "Waiting...")
      (go-loop []
        (when-let [p (<! dc)]
          (reset! progress p)
          (recur))))
    (go (reset! result (<! c)))))

(defonce n-in (r/atom 100))
(defonce t-red (r/atom 100))
(defonce t-in (r/atom 100))
(defonce np-max (r/atom 10))
(defonce do-rand (r/atom false))

(defn atom-input [name value vmin vmax]
  [:p name 
   [:input {:type "text"
            :value @value
            :on-change #(let [v (int  (-> % .-target .-value))]
                          (when (and (<= v vmax) (>= v vmin))
                            (reset! value v)))}]])

(defn reduce-page []
  [:div
   [:div {:style {:float "left"}}
    (atom-input "N=" n-in 1 1000)
    [:input {:type "checkbox"  :checked @do-rand  :on-change #(swap! do-rand not)}]
    (str "Randomize time=" @do-rand)
    (atom-input "Input delay=" t-in 1 1000)
    (atom-input "Reduction delay=" t-red 1 1000)
    (atom-input "Concurrency=" np-max 1 100)
    [:div [:button {:on-click (fn [] (start-reduce @n-in @do-rand @t-in @t-red @np-max))} "Reduce!"]
     "=" (str @result)]]
   [:div {:style {:background-color "white" :float "left"}}
    [:svg {:height 200}
     [:g {:key "bleh"} [:text {:x 0 :y 30} (str "n=" (:n @progress) ", np=" (:np @progress))  ]
      (doall (for [i (range 10)]
               (let [y (+ 50 (* 15 i))
                     w  (+ 1 (* 10 (-> @progress :queues (nth i) second)))
                     w2 (+ 1 (* 10 (-> @progress :queues (nth i) (nth 2))))]
                 [:g {:key i}
                  [:text {:key (str "text" i) :x 35 :y (+ 10  y) :fill "red" :height 2 :width 40 :font-size 10 :text-anchor "end"} (str  (Math/pow 2 i))]
                  [:rect {:key (str "rect" i) :height 10 :width w :x 40 :y y :fill "blue"}]
                  [:rect {:key (str "rect2" i) :height 10 :width w2 :x 40 :y y :fill "red"}]]
                 )))]]]
   ])

(defn current-page []
  [:div [(session/get :current-page)]])

;; -------------------------
;; Routes

(secretary/defroute "/" []
  (session/put! :current-page #'reduce-page))


;; -------------------------
;; Initialize app

(defn mount-root []
  (r/render [current-page] (.getElementById js/document "app")))

(defn init! []
  (accountant/configure-navigation!
    {:nav-handler
     (fn [path]
       (secretary/dispatch! path))
     :path-exists?
     (fn [path]
       (secretary/locate-route path))})
  (accountant/dispatch-current!)
  (mount-root))

(defn ^:export run []
  (r/render [reduce-page]
            (js/document.getElementById "app")))
