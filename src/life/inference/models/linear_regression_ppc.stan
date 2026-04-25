data {
  int<lower=1> N;
  int<lower=1> K;
  matrix[N, K] X;
  vector[N] y;
}

parameters {
  real alpha;
  vector[K] beta;
  real<lower=0> sigma;
}

model {
  alpha ~ normal(0, 2.5);
  beta ~ normal(0, 1.5);
  sigma ~ exponential(1);
  y ~ normal(alpha + X * beta, sigma);
}

generated quantities {
  vector[N] mu;
  array[N] real y_rep;
  array[N] real log_lik;
  for (n in 1:N) {
    mu[n] = alpha + X[n] * beta;
    y_rep[n] = normal_rng(mu[n], sigma);
    log_lik[n] = normal_lpdf(y[n] | mu[n], sigma);
  }
}
