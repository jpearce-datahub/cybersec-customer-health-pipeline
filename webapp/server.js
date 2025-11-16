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
        const filePath = path.join(__dirname, '..', 'data', 'raw', filename);
        
        if (!fs.existsSync(filePath)) {
            console.error(`File not found: ${filePath}`);
            resolve([]); // Return empty array instead of error
            return;
        }
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', (err) => {
                console.error(`Error reading ${filename}:`, err);
                resolve([]); // Return empty array on error
            });
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

// Security Analytics Endpoints
app.get('/api/security-kpis', async (req, res) => {
    try {
        const incidents = await loadCSV('security_incidents.csv');
        const kpis = calculateSecurityKPIs(incidents);
        res.json(kpis);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/ip-analysis', async (req, res) => {
    try {
        const incidents = await loadCSV('security_incidents.csv');
        const ipAnalysis = analyzeIPs(incidents);
        res.json(ipAnalysis);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/attack-patterns', async (req, res) => {
    try {
        const incidents = await loadCSV('security_incidents.csv');
        const patterns = analyzeAttackPatterns(incidents);
        res.json(patterns);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/network-analysis', async (req, res) => {
    try {
        const incidents = await loadCSV('security_incidents.csv');
        const networkAnalysis = analyzeNetworkSegments(incidents);
        res.json(networkAnalysis);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Security Analytics Functions
function calculateSecurityKPIs(incidents) {
    const total = incidents.length;
    const critical = incidents.filter(i => i['Severity Level'] === 'Critical').length;
    const high = incidents.filter(i => i['Severity Level'] === 'High').length;
    const prevented = incidents.filter(i => i['Action Taken'] === 'Blocked').length;
    const alerted = incidents.filter(i => i['Alerts/Warnings'] === 'Alert Triggered').length;
    
    return {
        totalIncidents: total,
        criticalIncidents: critical,
        highIncidents: high,
        preventionRate: total > 0 ? ((prevented / total) * 100).toFixed(2) : 0,
        alertCoverage: total > 0 ? ((alerted / total) * 100).toFixed(2) : 0,
        criticalRate: total > 0 ? ((critical / total) * 100).toFixed(2) : 0,
        securityPosture: critical > 0 ? 'Critical' : high > 5 ? 'High' : 'Good'
    };
}

function analyzeIPs(incidents) {
    const ipMap = new Map();
    
    incidents.forEach(incident => {
        const ip = incident['Source IP Address'];
        if (!ipMap.has(ip)) {
            ipMap.set(ip, {
                ip: ip,
                incidents: 0,
                customers: new Set(),
                attackTypes: new Set(),
                severities: []
            });
        }
        
        const ipData = ipMap.get(ip);
        ipData.incidents++;
        ipData.customers.add(incident.customer_id);
        ipData.attackTypes.add(incident['Attack Type']);
        ipData.severities.push(incident['Severity Level']);
    });
    
    return Array.from(ipMap.values()).map(ip => ({
        ...ip,
        customers: ip.customers.size,
        attackTypes: Array.from(ip.attackTypes),
        threatLevel: ip.customers.size >= 3 ? 'High Risk' : ip.incidents >= 5 ? 'Suspicious' : 'Low Risk'
    })).sort((a, b) => b.incidents - a.incidents).slice(0, 20);
}

function analyzeAttackPatterns(incidents) {
    const patterns = {
        byType: {},
        bySeverity: {},
        byHour: Array(24).fill(0),
        byDay: Array(7).fill(0)
    };
    
    incidents.forEach(incident => {
        const type = incident['Attack Type'];
        const severity = incident['Severity Level'];
        const timestamp = new Date(incident.Timestamp);
        const hour = timestamp.getHours();
        const day = timestamp.getDay();
        
        patterns.byType[type] = (patterns.byType[type] || 0) + 1;
        patterns.bySeverity[severity] = (patterns.bySeverity[severity] || 0) + 1;
        patterns.byHour[hour]++;
        patterns.byDay[day]++;
    });
    
    return patterns;
}

function analyzeNetworkSegments(incidents) {
    const segmentMap = new Map();
    
    incidents.forEach(incident => {
        const segment = incident['Network Segment'];
        if (!segmentMap.has(segment)) {
            segmentMap.set(segment, {
                segment: segment,
                incidents: 0,
                customers: new Set(),
                attackTypes: {},
                severities: {}
            });
        }
        
        const segData = segmentMap.get(segment);
        segData.incidents++;
        segData.customers.add(incident.customer_id);
        
        const attackType = incident['Attack Type'];
        const severity = incident['Severity Level'];
        segData.attackTypes[attackType] = (segData.attackTypes[attackType] || 0) + 1;
        segData.severities[severity] = (segData.severities[severity] || 0) + 1;
    });
    
    return Array.from(segmentMap.values()).map(seg => ({
        ...seg,
        customers: seg.customers.size,
        riskLevel: seg.severities['Critical'] > 0 ? 'Critical' : seg.incidents > 10 ? 'High' : 'Medium'
    })).sort((a, b) => b.incidents - a.incidents);
}

// Chatbot endpoint for intelligent queries
app.post('/api/chat', async (req, res) => {
    console.log('Chat endpoint hit with:', req.body);
    try {
        const { question } = req.body;
        if (!question) {
            return res.status(400).json({ error: 'Question is required' });
        }
        
        const [customers, tickets, incidents] = await Promise.all([
            loadCSV('customers.csv'),
            loadCSV('support_tickets.csv'),
            loadCSV('security_incidents.csv')
        ]);
        
        console.log('Data loaded:', { customers: customers.length, tickets: tickets.length, incidents: incidents.length });
        
        const response = processQuery(question, { customers, tickets, incidents });
        console.log('Response generated:', response);
        res.json({ response });
    } catch (error) {
        console.error('Chat error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Test endpoint
app.get('/api/test', (req, res) => {
    res.json({ message: 'Server is running' });
});

function processQuery(question, data) {
    const q = question.toLowerCase();
    
    // High risk customers analysis
    if (q.includes('highest risk customers') || q.includes('high risk')) {
        const highRiskCustomers = data.customers
            .filter(c => c.risk_score === 'high')
            .slice(0, 5)
            .map(c => ({
                id: c.customer_id,
                mrr: c.monthly_recurring_revenue,
                riskFactors: [
                    c.support_tickets_count > 10 ? 'High support volume' : null,
                    c.security_incidents_count > 5 ? 'Multiple security incidents' : null,
                    c.nps_score < 6 ? 'Low satisfaction' : null
                ].filter(Boolean)
            }));
        
        return {
            type: 'high-risk-analysis',
            data: highRiskCustomers,
            summary: `Found ${highRiskCustomers.length} highest risk customers. Key risk factors: high support volume, security incidents, and low satisfaction scores.`
        };
    }
    
    // Resolution time analysis
    if (q.includes('resolution time') || q.includes('driving') && q.includes('time')) {
        const ticketsByType = {};
        data.tickets.forEach(ticket => {
            const type = ticket.category || 'General';
            if (!ticketsByType[type]) {
                ticketsByType[type] = { count: 0, totalTime: 0, tickets: [] };
            }
            if (ticket.resolution_time_hours && ticket.resolution_time_hours !== 'NULL') {
                ticketsByType[type].count++;
                ticketsByType[type].totalTime += parseFloat(ticket.resolution_time_hours);
                ticketsByType[type].tickets.push(ticket);
            }
        });
        
        const avgByType = Object.entries(ticketsByType)
            .map(([type, data]) => ({
                type,
                avgTime: data.count > 0 ? (data.totalTime / data.count).toFixed(1) : 0,
                count: data.count
            }))
            .sort((a, b) => b.avgTime - a.avgTime)
            .slice(0, 5);
        
        return {
            type: 'resolution-analysis',
            data: avgByType,
            summary: `${avgByType[0]?.type} tickets have the highest avg resolution time at ${avgByType[0]?.avgTime} hours, followed by ${avgByType[1]?.type} at ${avgByType[1]?.avgTime} hours.`
        };
    }
    
    // Security incident patterns
    if (q.includes('security') && (q.includes('pattern') || q.includes('trend'))) {
        const incidentsByCustomer = {};
        data.incidents.forEach(incident => {
            const customerId = incident.customer_id;
            if (!incidentsByCustomer[customerId]) {
                incidentsByCustomer[customerId] = { total: 0, critical: 0, types: {} };
            }
            incidentsByCustomer[customerId].total++;
            if (incident['Severity Level'] === 'Critical') {
                incidentsByCustomer[customerId].critical++;
            }
            const type = incident['Attack Type'];
            incidentsByCustomer[customerId].types[type] = (incidentsByCustomer[customerId].types[type] || 0) + 1;
        });
        
        const topRiskCustomers = Object.entries(incidentsByCustomer)
            .map(([id, data]) => ({ customerId: id, ...data }))
            .sort((a, b) => b.critical - a.critical || b.total - a.total)
            .slice(0, 5);
        
        return {
            type: 'security-patterns',
            data: topRiskCustomers,
            summary: `Top security risk customers have ${topRiskCustomers[0]?.critical} critical incidents. Most common attack types are Malware, DDoS, and Intrusion attempts.`
        };
    }
    
    // Customer satisfaction analysis
    if (q.includes('satisfaction') || q.includes('nps') || q.includes('feedback')) {
        const satisfactionData = data.customers.map(c => ({
            id: c.customer_id,
            nps: parseInt(c.nps_score) || 0,
            renewalLikelihood: c.likelihood_to_renew,
            supportTickets: parseInt(c.support_tickets_count) || 0,
            securityIncidents: parseInt(c.security_incidents_count) || 0
        })).sort((a, b) => a.nps - b.nps);
        
        const lowSatisfaction = satisfactionData.filter(c => c.nps <= 5).slice(0, 5);
        
        return {
            type: 'satisfaction-analysis',
            data: lowSatisfaction,
            summary: `${lowSatisfaction.length} customers have low satisfaction (NPS ≤ 5). Common factors: high support ticket volume and security incidents.`
        };
    }
    
    return {
        type: 'general',
        summary: 'I can help analyze customer health, support efficiency, and security incidents. Try asking about "highest risk customers", "resolution time drivers", or "security patterns".'
    };
}

app.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
});