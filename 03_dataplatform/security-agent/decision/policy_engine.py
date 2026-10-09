class PolicyEngine:

    def decide(self, risk_result):

        score = risk_result[
            "risk_score"
        ]

        if score >= 80:

            return {
                "action": "DISABLE_USER",
                "reason": (
                    "High risk brute force"
                ),
            }

        if score >= 50:

            return {
                "action": "ALERT",
                "reason": (
                    "Medium risk brute force"
                ),
            }

        return {
            "action": "LOG",
            "reason": (
                "Low risk suspicious activity"
            ),
        }