package org.benz3k3.javahacks.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.benz3k3.javahacks.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository

public class StudentJdbcRepository {

	@Autowired
	JdbcTemplate jdbcTemplate;

	class StudentRowMapper implements RowMapper<Student> {

		@Override

		public Student mapRow(ResultSet rs, int rowNum) throws SQLException {

			Student student = new Student();

			student.setId(rs.getLong("id"));

			student.setName(rs.getString("name"));

			student.setPassportNumber(rs.getString("passport_number"));

			return student;

		}

	}

	public List<Student> findAll() {

		return jdbcTemplate.query("select * from student", new StudentRowMapper());

	}
	
	public List<Student> find(long batchStart, long batchSize) {

		return jdbcTemplate.query(String.format("select * from student limit %s offset %s", batchStart, batchSize), new StudentRowMapper());

	}

	public Student findById(long id) {

		return jdbcTemplate.queryForObject("select * from student where id=?", new Object[] {

				id

		},

				new BeanPropertyRowMapper<Student>(Student.class));

	}

	public int deleteById(long id) {

		return jdbcTemplate.update("delete from student where id=?", new Object[] {

				id

		});

	}

	public int insert(Student student) {

		return jdbcTemplate.update("insert into oldstudent (id, name, passport_number) " + "values(?,  ?, ?)",

				new Object[] {

						student.getId(), student.getName(), student.getPassportNumber()

				});

	}

	public int[] batchInsert(List<Student> students) {

		return jdbcTemplate.batchUpdate("insert into oldstudent (id, name, passport_number) " + "values(?,  ?, ?)",

				new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						ps.setLong(1, students.get(i).getId());
						ps.setString(2, students.get(i).getName());
						ps.setString(3, students.get(i).getPassportNumber());

					}

					@Override
					public int getBatchSize() {
						// TODO Auto-generated method stub
						return students.size();
					}
				});

	}

	public int update(Student student) {

		return jdbcTemplate.update("update student " + " set name = ?, passport_number = ? " + " where id = ?",

				new Object[] {

						student.getName(), student.getPassportNumber(), student.getId()

				});

	}

}