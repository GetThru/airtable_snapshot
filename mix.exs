defmodule AirtableSnapshot.MixProject do
  use Mix.Project

  def project do
    [
      app: :airtable_snapshot,
      version: "0.2.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:httpotion, "~> 3.1.0"},
      {:jason, "~> 1.1"},
      {:poison, "~> 3.0"},
      {:hackney, "~> 1.9"}
    ]
  end
end
