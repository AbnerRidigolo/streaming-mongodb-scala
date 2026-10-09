package br.com.olist.deliverysla

/** Brazilian states (UF) to IBGE macro-regions. */
object Regions {
  private val ByRegion: Map[String, Seq[String]] = Map(
    "Norte" -> Seq("AC", "AP", "AM", "PA", "RO", "RR", "TO"),
    "Nordeste" -> Seq("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"),
    "Centro-Oeste" -> Seq("DF", "GO", "MT", "MS"),
    "Sudeste" -> Seq("ES", "MG", "RJ", "SP"),
    "Sul" -> Seq("PR", "RS", "SC")
  )

  private val ByState: Map[String, String] =
    for ((region, states) <- ByRegion; state <- states) yield state -> region

  /** Region of a two-letter state code; None for unknown codes ("NA"). */
  def of(state: String): Option[String] = ByState.get(state.trim.toUpperCase)
}
