select msa, sum(n) as n from t group by msa order by n desc limit 20;
