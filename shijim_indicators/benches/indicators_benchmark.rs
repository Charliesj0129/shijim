use criterion::{black_box, criterion_group, criterion_main, Criterion};
use shijim_indicators::RustVpinCalculator;
use shijim_indicators::RustHawkesIntensity;

fn benchmark_vpin(c: &mut Criterion) {
    c.bench_function("vpin_update", |b| {
        let mut calc = RustVpinCalculator::new(1000.0, 50).unwrap();
        let mut i = 0.0;
        b.iter(|| {
            i += 1.0;
            let val = if i % 2.0 == 0.0 { 10.0 } else { -10.0 };
            black_box(calc.update_signed_volume(val).unwrap());
        })
    });
}

fn benchmark_hawkes(c: &mut Criterion) {
    c.bench_function("hawkes_update", |b| {
        let mut calc = RustHawkesIntensity::new(0.1, 0.5, 1.0).unwrap();
        let mut t = 0.0;
        b.iter(|| {
            t += 0.001;
            black_box(calc.update(t).unwrap());
        })
    });
}

criterion_group!(benches, benchmark_vpin, benchmark_hawkes);
criterion_main!(benches);
