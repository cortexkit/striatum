use std::process::{Command, Stdio};

// The harness resets the consumer catalog and account quota state, both of
// which are process-wide. Run cases serially within each child, and keep the
// number of children bounded because nextest also runs tests in parallel.
const SHARDS: usize = 4;
const SHARD_ENV: &str = "CK_STRIATUM_TEST_CASE_SHARD";
const RECEIPT: &str = "CK_STRIATUM_TEST_CASE_COUNTS=";

pub(in crate::routing_v1) struct CaseShard(usize);

impl CaseShard {
    pub(in crate::routing_v1) fn current() -> Option<Self> {
        std::env::var_os(SHARD_ENV).map(|value| {
            let index = value.to_str().unwrap().parse().unwrap();
            assert!(index < SHARDS, "invalid case shard {index}");
            Self(index)
        })
    }

    pub(in crate::routing_v1) fn contains(&self, case_index: usize) -> bool {
        case_index % SHARDS == self.0
    }

    pub(in crate::routing_v1) fn report(&self, counts: &[usize]) {
        println!(
            "\n{RECEIPT}{}",
            serde_json::to_string(&(self.0, counts)).unwrap()
        );
    }
}

pub(in crate::routing_v1) fn run<const N: usize>(
    module: &str,
    test: &str,
    configure: impl Fn(&mut Command),
) -> [usize; N] {
    let (_, module) = module.split_once("::").unwrap();
    let name = format!("{module}::{test}");
    let exe = crate::test_exe::current_test_exe().unwrap();
    let outputs = std::thread::scope(|scope| {
        let children: Vec<_> = (0..SHARDS)
            .map(|index| {
                let mut command = Command::new(&exe);
                command
                    .args(["--exact", &name, "--nocapture"])
                    .env(SHARD_ENV, index.to_string())
                    .stdin(Stdio::null());
                configure(&mut command);
                scope.spawn(move || command.output())
            })
            .collect();
        children
            .into_iter()
            .map(|child| child.join().unwrap())
            .collect::<Vec<_>>()
    });
    let mut total = [0; N];
    for (index, output) in outputs.into_iter().enumerate() {
        let output = output.unwrap_or_else(|err| panic!("{name} shard {index} did not run: {err}"));
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{name} shard {index} failed: {}\n{stdout}\n{stderr}",
            output.status
        );
        let mut receipts = stdout.lines().filter_map(|line| line.strip_prefix(RECEIPT));
        let (reported, counts): (usize, Vec<usize>) = receipts
            .next()
            .and_then(|receipt| serde_json::from_str(receipt).ok())
            .unwrap_or_else(|| {
                panic!("{name} shard {index} has no valid count report\n{stdout}\n{stderr}")
            });
        assert_eq!(
            reported, index,
            "{name} shard {index} reported a different index"
        );
        assert!(
            receipts.next().is_none(),
            "{name} shard {index} reported twice"
        );
        assert_eq!(
            counts.len(),
            N,
            "{name} shard {index} reported the wrong number of counts"
        );
        for (sum, count) in total.iter_mut().zip(counts) {
            *sum += count;
        }
    }
    total
}

/// Add the case label to panics from assertions inside handlers and setup,
/// not just the final assertion in the case loop.
pub(in crate::routing_v1) struct CaseLabel<F: Fn() -> String>(pub F);

impl<F: Fn() -> String> Drop for CaseLabel<F> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("failing case: {}", (self.0)());
        }
    }
}
