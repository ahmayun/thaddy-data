
(set-logic QF_ASNIA)
(set-option :produce-models true)
(set-option :strings-exp true)


(define-fun isinteger ((x!1 String)) Bool (str.in_re x!1 (re.+ (re.range "0" "9")))  )
(define-fun notinteger ((x!1 String)) Bool (not (isinteger x!1)) )
(define-fun real.str.from_int ((i Int)) String (ite (< i 0) (str.++ "-" (str.from_int (- i))) (str.from_int i)))
(define-fun real.str.to_int ((i String)) Int (ite (= (str.substr i 0 1) "-") (- (str.to_int (str.substr i 1 (- (str.len i) 1)))) (str.to_int i)))

(declare-fun input2_d2 () String)
(declare-fun input2 () String)
(declare-fun input2_d4 () String)
(declare-fun input2_d1 () String)
(declare-fun input2_d0 () String)
(declare-fun input2_d3 () String)




     
(declare-fun input2_d5 () String)
(assert (= input2 (str.++ (str.++ (str.++ (str.++ (str.++ input2_d0 (str.++ "," input2_d1)) (str.++ "," input2_d2)) (str.++ "," input2_d3)) (str.++ "," input2_d4)) (str.++ "," input2_d5))))
(assert (>= (str.len input2_d4) 4)) 
(assert  (and (>=  (-  ( str.to_int ( str.substr input2_d4  0 (- 4 0) )  ) ( str.to_int input2_d1  ) ) 10 )  (and (isinteger  input2_d1 )  (and (isinteger  ( str.substr input2_d4  0 (- 4 0) ) )  (and (>  (-  ( str.to_int input2_d2  ) ( str.to_int input2_d3  ) ) 5000 )  (and (isinteger  input2_d3 ) (isinteger  input2_d2 ) ) ) ) ) ) )
(check-sat)
(get-model)
