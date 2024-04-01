package com.atypon.decentraldbcluster.query.documents;

import com.atypon.decentraldbcluster.query.base.Query;

public class DocumentQuery extends Query {
    private DocumentAction documentAction;
    private String condition;
    private String content;
    private String newContent;

    //------------------------- Getter And Setter

    public DocumentAction getDocumentAction() {
        return documentAction;
    }

    public void setDocumentAction(DocumentAction documentAction) {
        this.documentAction = documentAction;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getNewContent() {
        return newContent;
    }

    public void setNewContent(String newContent) {
        this.newContent = newContent;
    }

    @Override
    public String toString() {
        return "DocumentQuery{" +
                "documentAction=" + documentAction +
                ", condition='" + condition + '\'' +
                ", content='" + content + '\'' +
                ", newContent='" + newContent + '\'' +
                ", originator='" + originator + '\'' +
                ", database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                '}';
    }
}
