const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const app = express();
const PORT = 3000;

app.use(express.static('public'));
app.use(express.json());

// Load CSV data
function loadCSV(filename) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(path.join('..', 'data', 'raw', filename))
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', reject);
    });
}

// API Routes
app.get('/api/dashboard', async (req, res) => {
    try {
        const customers = await loadCSV('customers.csv');
        const tickets = await loadCSV('support_tickets.csv');
        const incidents = await loadCSV('security_incidents.csv');
        const feedback = await loadCSV('customer_feedback.csv');

        const dashboard = {
            totalCustomers: customers.length,
            highRiskCustomers: customers.filter(c => c.risk_score === 'high').length,
            totalMRR: customers.reduce((sum, c) => sum + parseFloat(c.monthly_recurring_revenue), 0),
            resolvedTickets: tickets.filter(t => t.status === 'Resolved').length,
            totalTickets: tickets.length,
            avgResolutionTime: (() => {
                const validTickets = tickets.filter(t => t.resolution_time_hours && t.resolution_time_hours !== 'NULL');
                return validTickets.length > 0 ? 
                    validTickets.reduce((sum, t) => sum + parseFloat(t.resolution_time_hours), 0) / validTickets.length : 0;
            })(),
            escalatedTickets: tickets.filter(t => t.escalated === 'TRUE').length,
            criticalIncidents: incidents.filter(i => i.severity === 'Critical').length,
            totalIncidents: incidents.length,
            avgNPS: feedback.length > 0 ? feedback.reduce((sum, f) => sum + parseInt(f.nps_score), 0) / feedback.length : 0,
            highRenewal: feedback.filter(f => f.likelihood_to_renew === 'High').length
        };

        res.json(dashboard);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/customers', async (req, res) => {
    try {
        const customers = await loadCSV('customers.csv');
        res.json(customers);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/tickets', async (req, res) => {
    try {
        const tickets = await loadCSV('support_tickets.csv');
        res.json(tickets);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
});