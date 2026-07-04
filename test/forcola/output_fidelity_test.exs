defmodule Forcola.OutputFidelityTest do
  # async: false -- consistent with the other suites; the large-output
  # tests should not compete with other shim traffic for scheduling.
  use ExUnit.Case, async: false

  # seq stays within 6 significant digits so BSD seq's %g formatting
  # matches GNU seq byte for byte (1_000_000 would print as "1e+06" on
  # macOS). ~3.4 MB total: larger than any pipe buffer, so the shim's
  # output pumps and frame segmentation are exercised for real.
  @count 500_000

  describe "large output" do
    test "run/2 captures multi-megabyte stdout byte-exactly" do
      assert {:ok, %Forcola.Result{status: 0, stdout: stdout, stderr: ""}} =
               Forcola.run(["/bin/sh", "-c", "seq 1 #{@count}"], timeout_ms: 30_000)

      assert byte_size(stdout) == expected_seq_bytes()
      assert String.ends_with?(stdout, "#{@count}\n")
    end

    test "Stream.lines delivers every line of multi-megabyte output" do
      {count, bytes, last} =
        ["/bin/sh", "-c", "seq 1 #{@count}"]
        |> Forcola.Stream.lines(timeout_ms: 30_000)
        |> Enum.reduce({0, 0, nil}, fn line, {count, bytes, _last} ->
          {count + 1, bytes + byte_size(line), line}
        end)

      assert count == @count
      # Lines carry no trailing newline, so add one back per line.
      assert bytes + count == expected_seq_bytes()
      assert last == "#{@count}"
    end
  end

  describe "invalid UTF-8 and NUL bytes" do
    # Octal escapes, not \xHH: dash's printf (the /bin/sh builtin on
    # Ubuntu CI) only supports the POSIX \ddd form.
    @dirty_script ~S(printf 'ok1\n\377\376 bad\nwith\0nul\nok2\n')
    @dirty_lines ["ok1", <<0xFF, 0xFE, " bad">>, <<"with", 0, "nul">>, "ok2"]

    test "Stream.lines passes them through byte-exactly" do
      lines =
        ["/bin/sh", "-c", @dirty_script]
        |> Forcola.Stream.lines(timeout_ms: 5_000)
        |> Enum.to_list()

      assert lines == @dirty_lines
      refute String.valid?(Enum.at(lines, 1)), "test bytes were unexpectedly valid UTF-8"
    end

    test "Duplex delivers them byte-exactly as :forcola_line messages" do
      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", @dirty_script], kill_grace_ms: 1_000)

      for expected <- @dirty_lines do
        assert_receive {:forcola_line, ^session, ^expected}, 10_000
      end

      assert_receive {:forcola_exit, ^session, 0}, 10_000
    end
  end

  defp expected_seq_bytes do
    Enum.reduce(1..@count, 0, fn n, acc ->
      acc + byte_size(Integer.to_string(n)) + 1
    end)
  end
end
