use numpy::PyReadonlyArray1;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

const MIN_TIME_EPS: f64 = 1e-12;

#[pyclass]
pub struct RustHawkesIntensity {
    baseline: f64,
    alpha: f64,
    beta: f64,
    last_intensity: f64,
    last_timestamp: Option<f64>,
}

#[pymethods]
impl RustHawkesIntensity {
    #[new]
    #[pyo3(text_signature = "(baseline, alpha, beta)")]
    pub fn new(baseline: f64, alpha: f64, beta: f64) -> PyResult<Self> {
        if !baseline.is_finite() || baseline < 0.0 {
            return Err(PyValueError::new_err(
                "baseline intensity must be finite and >= 0",
            ));
        }
        if !alpha.is_finite() || alpha < 0.0 {
            return Err(PyValueError::new_err("alpha must be finite and >= 0"));
        }
        if !beta.is_finite() || beta <= 0.0 {
            return Err(PyValueError::new_err("beta must be finite and > 0"));
        }

        Ok(Self {
            baseline,
            alpha,
            beta,
            last_intensity: baseline,
            last_timestamp: None,
        })
    }

    pub fn reset(&mut self) {
        self.last_intensity = self.baseline;
        self.last_timestamp = None;
    }

    pub fn update(&mut self, timestamp: f64) -> PyResult<f64> {
        Self::validate_timestamp(timestamp)?;
        if let Some(last_ts) = self.last_timestamp {
            if timestamp + MIN_TIME_EPS < last_ts {
                return Err(PyValueError::new_err(
                    "timestamps must be non-decreasing for Hawkes updates",
                ));
            }
            let dt = (timestamp - last_ts).max(0.0);
            let decayed = self.decayed_intensity(dt);
            self.last_intensity = decayed + self.alpha;
        } else {
            self.last_intensity = self.baseline + self.alpha;
        }
        self.last_timestamp = Some(timestamp);
        Ok(self.last_intensity)
    }

    pub fn update_many<'py>(
        &mut self,
        timestamps: PyReadonlyArray1<'py, f64>,
    ) -> PyResult<Vec<f64>> {
        let slice = timestamps.as_slice()?;
        let mut out = Vec::with_capacity(slice.len());
        for &ts in slice {
            out.push(self.update(ts)?);
        }
        Ok(out)
    }

    pub fn current_intensity(&self) -> f64 {
        self.last_intensity
    }

    pub fn intensity_at(&self, timestamp: f64) -> PyResult<f64> {
        Self::validate_timestamp(timestamp)?;
        if let Some(last_ts) = self.last_timestamp {
            if timestamp + MIN_TIME_EPS < last_ts {
                return Err(PyValueError::new_err(
                    "query timestamp must be >= last processed event",
                ));
            }
            let dt = (timestamp - last_ts).max(0.0);
            Ok(self.decayed_intensity(dt))
        } else {
            Ok(self.baseline)
        }
    }
}

impl RustHawkesIntensity {
    fn decayed_intensity(&self, dt: f64) -> f64 {
        if dt <= 0.0 {
            return self.last_intensity;
        }
        let decay = (-self.beta * dt).exp();
        self.baseline + (self.last_intensity - self.baseline) * decay
    }

    fn validate_timestamp(timestamp: f64) -> PyResult<()> {
        if !timestamp.is_finite() {
            return Err(PyValueError::new_err(
                "timestamps supplied to Hawkes calculator must be finite",
            ));
        }
        Ok(())
    }
}
