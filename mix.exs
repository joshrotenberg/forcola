defmodule Forcola.MixProject do
  use Mix.Project

  def project do
    [
      app: :forcola,
      version: "0.1.0",
      elixir: "~> 1.18",
      compilers: Mix.compilers() ++ [:forcola_shim],
      start_permanent: Mix.env() == :prod,
      description:
        "Leak-free external process execution: process-group kill on timeout or BEAM death via a precompiled Rust shim",
      source_url: "https://github.com/joshrotenberg/forcola",
      homepage_url: "https://github.com/joshrotenberg/forcola",
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/joshrotenberg/forcola"}
    ]
  end

  defp docs do
    [
      main: "Forcola",
      source_url: "https://github.com/joshrotenberg/forcola",
      extras: ["README.md"],
      groups_for_modules: [
        "Execution modes": [Forcola, Forcola.Stream, Forcola.Daemon, Forcola.Duplex],
        "Data structures": [Forcola.Result],
        Internals: [Forcola.Shim]
      ]
    ]
  end
end
