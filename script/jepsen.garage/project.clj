(defproject jepsen.garage "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Garage"
  :url "https://git.deuxfleurs.fr/Deuxfleurs/garage"
  :license {:name "GPLv3"
            :url "https://www.gnu.org/licenses/gpl-3.0.en.html"}
  :main jepsen.garage
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.2-SNAPSHOT"]
                 [amazonica "0.3.163"]]
  :repl-options {:init-ns jepsen.garage})
