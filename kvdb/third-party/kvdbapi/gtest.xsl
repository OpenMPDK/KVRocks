<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:msxsl="urn:schemas-microsoft-com:xslt" exclude-result-prefixes="msxsl"> 

<xsl:output method="html" indent="yes"/> 

<xsl:template match="/"> 

<table cellpadding="2" cellspacing="5" border="1px">
<tr>
    <th bgcolor="#808080"><font color="#FFFFFF">Testcase Num</font></th>
    <th bgcolor="#808080"><font color="#FFFFFF">Failure Num</font></th>
</tr>
<tr>
    <td style="font-family: Verdana; font-size: 15px; font-weight: bold;"><xsl:value-of select="testsuites/@tests"/> </td>
    <td style="font-family: Verdana; font-size: 15px; font-weight: bold;"><xsl:value-of select="testsuites/@failures"/> </td>
</tr>
</table>

<table cellpadding="2" cellspacing="5"> 
<tr><td style="font-family: Verdana; font-size: 10px;">

<table align="left" cellpadding="2" cellspacing="0" style="font-family: Verdana; font-size: 10px;"> 
<tr>
<th bgcolor="#808080"><font color="#FFFFFF"><b>TestSuites</b></font></th> 
<th bgcolor="#808080">
<table width="1000px" align="left" cellpadding="1" cellspacing="0" style="font-family: Verdana; font-size: 10px;">
<tr style="font-family: Verdana; font-size: 10px;">
<td  width="15%"><font color="#FFFFFF"><b>Testcase</b></font></td>
<td  width="25%"><font color="#FFFFFF"><b>Result</b></font></td>
<td  width="75%"><font color="#FFFFFF"><b>ErrorInfo</b></font></td>
</tr>
</table>
</th> 
</tr> 
<xsl:for-each select="testsuites/testsuite"> 
<tr>
<td style="border: 1px solid #808080"><xsl:value-of select="@name"/></td> 
<td style="border: 1px solid #808080">
<table width="1000px" align="left" cellpadding="1" cellspacing="0" style="font-family: Verdana; font-size: 10px;">
<xsl:for-each select="testcase">
<tr>
<td style="border: 1px solid #808080" width="15%" rowspan="@tests"><xsl:value-of select="@name"/></td>
<xsl:choose>
    <xsl:when test="failure">
      <td style="border: 1px solid #808080" bgcolor="#ff00ff" width="25%">Failure</td>
      <td style="border: 1px solid #808080" bgcolor="#ff00ff" width="70%"><xsl:value-of select="failure/@message"/></td>
    </xsl:when>
    <xsl:otherwise>
     <td style="border: 1px solid #808080" width="25%">Success</td>
     <td style="border: 1px solid #808080" width="70%"><xsl:value-of select="failure/@message"/></td>
     </xsl:otherwise>
</xsl:choose>
</tr>
</xsl:for-each>
</table>
</td> 
</tr>
</xsl:for-each> 
</table> 
</td> 
</tr> 
</table>

</xsl:template>
</xsl:stylesheet>
