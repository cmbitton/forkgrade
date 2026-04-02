from app.db import db


class Violation(db.Model):
    __tablename__ = 'violations'

    id = db.Column(db.Integer, primary_key=True)
    inspection_id = db.Column(
        db.Integer,
        db.ForeignKey('inspections.id', ondelete='CASCADE'),
        nullable=False
    )
    violation_code = db.Column(db.String(50))
    description = db.Column(db.Text)
    inspector_notes = db.Column(db.Text)
    severity = db.Column(db.String(20))  # critical, major, minor
    corrected_on_site = db.Column(db.Boolean, default=False)

    __table_args__ = (
        db.Index('ix_violations_inspection_id', 'inspection_id'),
    )

    def __repr__(self):
        return f'<Violation {self.violation_code} ({self.severity})>'
