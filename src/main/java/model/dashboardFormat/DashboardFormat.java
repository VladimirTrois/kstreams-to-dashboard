package model.dashboardFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

public abstract class DashboardFormat implements DashboardFormatable, Serializable
{
    protected String label;

    public DashboardFormat(String labelForDashboard)
    {
        this.label = labelForDashboard;
    }

    public String getLabel()
    {
        return label;
    }

    public void setLabel(String label)
    {
        this.label = label;
    }

    public String toDashboardFormatString()
    {
        String jsonString = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            jsonString = objectMapper.writeValueAsString(this);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return jsonString;
    }

    @Override public String toString()
    {
        return "{" +
                "\"label\":\"" + label + '\"' +
                '}';
    }
}
