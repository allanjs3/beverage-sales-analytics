"""Render notebook outcomes without querying data or changing execution results."""

from html import escape


def render_execution_summary(result, layer):
    """Return an escaped HTML summary for a completed Bronze or Silver run.

    Counts and observations come from existing checks. No Spark actions are
    started here. The original result remains available for job automation.
    """
    def text(value):
        return escape(str(value))

    def number(value):
        return f"{int(value):,}".replace(",", ".")

    if result["status"] == "NO_NEW_DATA":
        headline = "Nenhuma linha adicionada nesta execução"
    elif result.get("review_required"):
        headline = "Carga concluída — revisão necessária"
    else:
        headline = "Carga concluída"

    rows, observations = [], []
    for source in result.get("sources", []):
        name = source.get("target", source["source"]).split(".")[-1]
        added = source["rows_added"]
        needs_review = False
        if added and layer == "Bronze":
            rescued = source.get("rescued_rows", 0)
            if rescued:
                needs_review = True
                observations.append(
                    f"{name}: {number(rescued)} registros com campos resgatados pelo leitor; revisar o conteúdo."
                )
            for check in source.get("reference_checks", []):
                if not check["matches"]:
                    needs_review = True
                    observations.append(
                        f"{name}: arquivo {check['file']} recebeu {number(check['actual_rows'])} "
                        f"registros; eram esperados {number(check['expected_rows'])}."
                    )
        elif added and layer == "Silver":
            warnings = result.get("quality_warnings", {}).get(source["source"], {})
            negative = warnings.get("negative_sales_amount", 0)
            zero = warnings.get("zero_sales_amount", 0)
            if negative or zero:
                observations.append(
                    f"{name}: {number(negative)} valores negativos e {number(zero)} valores zero "
                    "preservados conforme as regras."
                )
        state = "Sem novas linhas" if not added else "Revisar" if needs_review else "Sem divergências"
        rows.append(
            f"<tr><td>{text(name)}</td><td style='text-align:right'>{number(added)}</td>"
            f"<td>{text(state)}</td></tr>"
        )

    table = ""
    if rows:
        table = (
            "<table><thead><tr><th>Tabela</th><th>Registros adicionados</th><th>Conferência</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
        )
    notes = ""
    if observations:
        notes = "<ul>" + "".join(f"<li>{text(note)}</li>" for note in observations) + "</ul>"
    return (
        "<style>.execution-summary{font:14px system-ui,sans-serif;color:#253244;max-width:900px}"
        ".execution-summary h3{font-size:20px;margin:0 0 18px}"
        ".execution-summary table{border-collapse:collapse;width:100%}"
        ".execution-summary th,.execution-summary td{padding:10px 14px;border-bottom:1px solid #dce3eb;text-align:left}"
        ".execution-summary th{background:#f3f6fa}.execution-summary li{margin:8px 0}</style>"
        f"<div class='execution-summary'><h3>{text(layer)} — {text(headline)}</h3>{table}{notes}</div>"
    )


def render_gold_summary(result):
    """Distinguish added fact rows from rewritten summary groups."""
    rows = []
    for item in result["sources"]:
        status = "Atualizado" if item["status"] == "UPDATED" else "Sem novas linhas na origem"
        values = [item["table"], item["measure"], f"{item['rows']:,}".replace(",", "."), status]
        rows.append("<tr>" + "".join(f"<td style='padding:10px'>{escape(v)}</td>" for v in values) + "</tr>")
    title = "Gold — processamento concluído" if result["has_new_data"] else "Gold — sem novas linhas"
    return (f"<h3>{title}</h3><table style='width:100%;text-align:left'>"
            "<tr style='background:#f1f5f9'><th>Tabela</th><th>Medida</th><th>Quantidade</th>"
            "<th>Resultado</th></tr>" + "".join(rows) + "</table>")
