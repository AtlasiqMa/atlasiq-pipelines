# Central source of truth for all Moroccan airport codes
# Used by both extractors and transformers

MOROCCAN_AIRPORTS = {
    "Casablanca":   "CMN",
    "Marrakech":    "RAK",
    "Agadir":       "AGA",
    "Tangier":      "TNG",
    "Fes":          "FEZ",
    "Rabat":        "RBA",
    "Oujda":        "OUD",
    "Essaouira":    "ESU",
    "Laayoune":     "EUN",
    "Al Hoceima":   "AHU",
    "Dakhla":       "VIL",
    "Nador":        "NDR",
    "Tetouan":      "TTU",
    "Errachidia":   "ERH",
    "Ouarzazate":   "OZZ",
    "Tan Tan":      "TTA",
    "Smara":        "SMW",
    "Guelmim":      "GLN",
}

# Set of IATA codes for fast lookup
MOROCCAN_IATAS = set(MOROCCAN_AIRPORTS.values())

# Major airports we actively extract from (rate limit friendly)
MOROCCAN_AIRPORTS_EXTRACT = {
    "Casablanca":   "CMN",
    "Marrakech":    "RAK",
    "Agadir":       "AGA",
    "Tangier":      "TNG",
    "Fes":          "FEZ",
    "Rabat":        "RBA",
    "Oujda":        "OUD",
    "Essaouira":    "ESU",
}