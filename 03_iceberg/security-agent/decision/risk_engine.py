class RiskEngine:

    def calculate(self, incident):

        score = 0

        failed_attempts = incident.get(
            "failed_attempts",
            0,
        )

        ip_address = incident.get(
            "ip_address"
        )

        # --------------------------------
        # Failed login count
        # --------------------------------

        if failed_attempts >= 5:
            score += 30

        if failed_attempts >= 10:
            score += 20

        if failed_attempts >= 20:
            score += 20

        # --------------------------------
        # IP 존재
        # --------------------------------

        if ip_address:
            score += 10

        # --------------------------------
        # 최종 점수
        # --------------------------------

        score = min(score, 100)

        # --------------------------------
        # Risk Level
        # --------------------------------

        if score >= 80:

            level = "HIGH"

        elif score >= 50:

            level = "MEDIUM"

        else:

            level = "LOW"

        return {
            **incident,
            "risk_score": score,
            "risk_level": level,
        }