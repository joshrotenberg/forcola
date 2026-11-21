# Temporary probe for #149: what does the producer do when nobody reads?
dir = System.tmp_dir!()
out = Path.join(dir, "probe149.txt")
File.rm(out)

script = ~S"""
exec 3>"$OUT"
echo "sh=$(readlink /proc/$$/exe) fd2=$(readlink /proc/$$/fd/2)" >&3
(head --version 2>/dev/null | head -1) >&3
cat /proc/$$/fdinfo/2 >&3 2>/dev/null
yes x | head -c 1048576 >&2
echo "head_rc=$? at=$(date +%s.%N)" >&3
"""

t0 = System.monotonic_time(:millisecond)

{:ok, session} =
  Forcola.Duplex.open(["/bin/sh", "-c", script],
    delivery: :pull,
    max_line_bytes: 64,
    max_pending_bytes: 65,
    max_output_bytes: 2_000_000,
    env: [{"OUT", out}],
    kill_grace_ms: 100
  )

Process.sleep(500)
IO.puts("--- after 500 ms with no demand (t=#{System.monotonic_time(:millisecond) - t0} ms)")
IO.puts(File.read!(out))
IO.inspect(Forcola.Duplex.shutdown(session), label: "shutdown", limit: 12)
