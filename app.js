const express = require("express");
const axios = require("axios");
require("dotenv").config();

const app = express();

app.get("/insights", async (req, res) => {
    try {
        const response = await axios.get(
            `https://graph.facebook.com/v19.0/${process.env.AD_ACCOUNT_ID}/insights`,
            {
                params: {
                    access_token: process.env.ACCESS_TOKEN,
                    fields: "campaign_name,impressions,clicks,spend"
                }
            }
        );
        res.json(response.data);
    } catch (err) {
        res.status(500).json(err.response?.data || err.message);
    }
});

app.listen(3000, () => console.log("Running on port 3000"));