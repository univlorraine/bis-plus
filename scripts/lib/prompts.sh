#!/bin/bash
###############################################################################
# scripts/lib/prompts.sh
# Fonctions de prompts utilisateur interactifs
###############################################################################

# Source les couleurs si pas déjà fait
SCRIPT_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -z "$NC" ]] && source "$SCRIPT_LIB_DIR/colors.sh"

# Demande une valeur avec valeur par défaut optionnelle
ask() {
    local prompt="$1"
    local default="$2"
    local result

    if [[ -n "$default" ]]; then
        echo -n "$prompt [$default] : " >&2
        read -r result </dev/tty
        echo "${result:-$default}"
    else
        echo -n "$prompt : " >&2
        read -r result </dev/tty
        echo "$result"
    fi
}

# Demande un secret (sans affichage)
ask_secret() {
    local prompt="$1"
    local result

    echo -n "$prompt : " >&2
    read -rs result </dev/tty
    echo "" >&2
    echo "$result"
}

# Demande une confirmation oui/non
ask_confirm() {
    local prompt="$1"
    local default="${2:-n}"
    local result

    if [[ "$default" =~ ^[Yy] ]]; then
        echo -n "$prompt [Y/n] : " >&2
    else
        echo -n "$prompt [y/N] : " >&2
    fi

    read -r result </dev/tty
    result="${result:-$default}"

    [[ "$result" =~ ^[Yy] ]]
}

# Demande un choix parmi une liste
# Usage: ask_choice "Titre" "default" "opt1:label1" "opt2:label2" ...
ask_choice() {
    local prompt="$1"
    local default="$2"
    shift 2
    local options=("$@")
    local result
    local i=1

    echo -e "${CYAN}$prompt${NC}" >&2
    for opt in "${options[@]}"; do
        local key="${opt%%:*}"
        local label="${opt#*:}"
        echo "  $i) $label" >&2
        ((i++))
    done
    echo -n "Votre choix [$default] : " >&2
    read -r result </dev/tty
    echo "${result:-$default}"
}

# Demande l'environnement (dev/prod)
ask_environment() {
    local default="${1:-1}"
    local result

    echo -e "${CYAN}Quel environnement souhaitez-vous configurer ?${NC}" >&2
    echo "  1) dev (sandbox - pour les tests)" >&2
    echo "  2) prod (production)" >&2
    echo -n "Votre choix [$default] : " >&2
    read -r result </dev/tty
    result="${result:-$default}"

    case "$result" in
        1|dev) echo "dev" ;;
        2|prod) echo "prod" ;;
        *) echo "dev" ;;
    esac
}
