defmodule Forcola.EscriptTest do
  use ExUnit.Case, async: false

  @moduletag :tmp_dir
  @tag timeout: 120_000
  test "a relocated Mix escript uses the bundled shim in all modes and reaps groups", %{
    tmp_dir: tmp_dir
  } do
    project = Path.join(tmp_dir, "consumer")
    relocated = Path.join(tmp_dir, "relocated")
    private_parent = Path.join(relocated, "private")
    File.mkdir_p!(Path.join(project, "lib"))
    File.mkdir_p!(private_parent)
    File.chmod!(private_parent, 0o700)
    File.cp!("test/fixtures/escript/cli_helper.exs", Path.join(project, "lib/cli.ex"))

    File.write!(Path.join(project, "mix.exs"), """
    defmodule ForcolaEscript.MixProject do
      use Mix.Project

      def project do
        [
          app: :forcola_escript,
          version: "0.0.0",
          escript: [
            main_module: ForcolaEscript.CLI,
            embed_elixir: true,
            include_priv_for: [:forcola]
          ],
          deps: [{:forcola, path: #{inspect(File.cwd!())}}]
        ]
      end
    end
    """)

    on_exit(fn -> cleanup_groups(private_parent) end)

    {build_output, build_status} =
      System.cmd("mix", ["escript.build"],
        cd: project,
        env: [{"MIX_ENV", "prod"}, {"MIX_BUILD_PATH", nil}, {"MIX_DEPS_PATH", nil}],
        stderr_to_stdout: true
      )

    assert build_status == 0, build_output
    executable = Path.join(relocated, "forcola_escript")
    File.rename!(Path.join(project, "forcola_escript"), executable)
    # Remove both the consumer source and its compiled dependency. Running in a
    # different cwd must only need the relocated archive and the OTP runtime.
    File.rm_rf!(project)

    {output, status} =
      System.cmd(executable, [private_parent], cd: relocated, stderr_to_stdout: true)

    assert status == 0, output
    assert output =~ "all modes and group cleanup passed"
    assert Enum.sort(File.ls!(private_parent)) == ["cancel-pids", "timeout-pids"]
  end

  defp cleanup_groups(directory) do
    for file <- Path.wildcard(Path.join(directory, "*-pids")),
        pid <- file |> File.read!() |> String.split() do
      System.cmd("kill", ["-KILL", pid], stderr_to_stdout: true)
    end
  end
end
