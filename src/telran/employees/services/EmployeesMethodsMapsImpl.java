package telran.employees.services;

import telran.employees.dto.Employee;
import telran.employees.dto.ReturnCode;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.StreamSupport;
import java.io.*;
public class EmployeesMethodsMapsImpl implements EmployeesMethods {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public EmployeesMethodsMapsImpl(String fileName) {
		this.fileName = fileName;
	}

	private transient String fileName; //field won't be serialized
 private HashMap<Long, Employee> mapEmployees = new HashMap<>(); //key employee's id, value - employee
 private TreeMap<Integer, List<Employee>> employeesAge= new TreeMap<>(); //key - age, value - list of employees with the same age
 private TreeMap<Integer, List<Employee>> employeesSalary = new TreeMap<>(); //key - salary,
 //value - list of employees with the same salary
 private HashMap<String, List<Employee>> employeesDepartment = new HashMap<>();
 
 private static ReadWriteLock[] locks = { new ReentrantReadWriteLock(), 
		 new ReentrantReadWriteLock(), new ReentrantReadWriteLock(), new ReentrantReadWriteLock()};
 private static Lock[] readLocks= {locks[0].readLock(), locks[1].readLock(),
		 							locks[2].readLock(), locks[3].readLock() };
 private static Lock[] writeLocks= {locks[0].writeLock(), locks[1].writeLock(),
		 							locks[2].writeLock(), locks[3].writeLock() };
 /*
      0  - mapEmployees
      1  - employeesAge
      2  - employeesSalary
      3  - employeesDepartment
  */
 
	@Override
	public ReturnCode addEmployee(Employee empl) {
		try {
			readLocks[0].lock();
			if (mapEmployees.containsKey(empl.id)) {
				return ReturnCode.EMPLOYEE_ALREADY_EXISTS;
			}
			readLocks[0].unlock();
			Employee emplS = copyOneEmployee(empl);
			writeLocks[0].lock();
			mapEmployees.put(emplS.id, emplS);
			writeLocks[0].unlock();
			writeLocks[1].lock();
			employeesAge.computeIfAbsent(getAge(emplS), k -> new LinkedList<Employee>()).add(emplS);
			writeLocks[1].unlock();
			writeLocks[2].lock();
			employeesSalary.computeIfAbsent(emplS.salary, k -> new LinkedList<Employee>()).add(emplS);
			writeLocks[2].unlock();
			writeLocks[3].lock();
			employeesDepartment.computeIfAbsent(emplS.department, k -> new LinkedList<Employee>()).add(emplS);
			writeLocks[3].unlock();
		
		return ReturnCode.OK;
		} finally {
			readLocks[0].unlock();
			writeLocks[0].unlock();
			writeLocks[1].unlock();
			writeLocks[2].unlock();
			writeLocks[3].unlock();
		}
	}

	private Integer getAge(Employee emplS) {
		
		return (int)ChronoUnit.YEARS.between(emplS.birthDate, LocalDate.now());
	}

	@Override
	public ReturnCode removeEmployee(long id) {
		try {
			writeLocks[0].lock();
			Employee empl = mapEmployees.remove(id);
			if (empl == null) {
				return ReturnCode.EMPLOYEE_NOT_FOUND;
			}
			writeLocks[0].unlock();
			writeLocks[1].lock();			
			employeesAge.get(getAge(empl)).remove(empl);
			writeLocks[1].unlock();
			writeLocks[2].lock();			
			employeesSalary.get(empl.salary).remove(empl);
			writeLocks[2].unlock();
			writeLocks[3].lock();			
			employeesDepartment.get(empl.department).remove(empl);
			writeLocks[3].unlock();
			return ReturnCode.OK;
		} finally {
			writeLocks[0].unlock();
			writeLocks[1].unlock();
			writeLocks[2].unlock();
			writeLocks[3].unlock();
		}
	}

	@Override
	public Iterable<Employee> getAllEmployees() {
		try {
			readLocks[0].lock();
			return copyEmployees(mapEmployees.values());
		} finally {
			readLocks[0].unlock();
		}
	}

	private Iterable<Employee> copyEmployees(Collection<Employee> employees) {
		
		return employees.stream()
				.map(empl -> copyOneEmployee(empl))
				.toList();
	}

	private Employee copyOneEmployee(Employee empl) {
		return new Employee(empl.id, empl.name, empl.birthDate, empl.salary, empl.department);
	}

	@Override
	public Employee getEmployee(long id) {
		try {
			readLocks[0].lock();
			Employee empl = mapEmployees.get(id);
			return empl == null ? null : copyOneEmployee(empl);
		} finally {
			readLocks[0].unlock();
		}
	}

	@Override
	public Iterable<Employee> getEmployeesByAge(int ageFrom, int ageTo) {
		try {
			readLocks[1].lock();
			Collection<List<Employee>> lists =
					employeesAge.subMap(ageFrom, true, ageTo, true).values();
			List<Employee> employeesList = getCombinedList(lists);
			// V.R. ??
			readLocks[1].unlock();
			return copyEmployees(employeesList);
		} finally {
			readLocks[1].unlock();
		}
	}

	private List<Employee> getCombinedList(Collection<List<Employee>> lists) {
		
		return lists.stream().flatMap(List::stream).toList();
	}

	@Override
	public Iterable<Employee> getEmployeesBySalary(int salaryFrom, int salaryTo) {
		try {
			readLocks[2].lock();
			Collection<List<Employee>> lists =
					employeesSalary.subMap(salaryFrom, true, salaryTo, true).values();
			List<Employee> employeesList = getCombinedList(lists);
			// V.R. ??
			readLocks[2].unlock();
			return copyEmployees(employeesList);
		} finally {
			readLocks[2].unlock();
		}
	}

	@Override
	public Iterable<Employee> getEmployeesByDepartment(String department) {
		try {
			readLocks[3].lock();
			List<Employee> employees = employeesDepartment.getOrDefault(department, Collections.emptyList());		
			return employees.isEmpty() ? employees : copyEmployees(employees);
		} finally {
			readLocks[3].unlock();
		}
	}

	

	@Override
	public Iterable<Employee> getEmployeesByDepartmentAndSalary(String department,
			int salaryFrom, int salaryTo) {
		// V.R !!!!!!
			Iterable<Employee> employeesByDepartment = getEmployeesByDepartment(department);
			HashSet<Employee> employeesBySalary = new HashSet<>((List<Employee>)getEmployeesBySalary(salaryFrom, salaryTo));
		
			return StreamSupport.stream(employeesByDepartment.spliterator(), false)
				.filter(employeesBySalary::contains).toList();
	}

	@Override
	public ReturnCode updateSalary(long id, int newSalary) {
		try {
			readLocks[0].lock();
			Employee empl = mapEmployees.get(id);
			if (empl == null) {
				return ReturnCode.EMPLOYEE_NOT_FOUND;
			}
			if (empl.salary == newSalary) {
				return ReturnCode.SALARY_NOT_UPDATED;
			}
			readLocks[0].unlock();
			writeLocks[2].lock();
			employeesSalary.get(empl.salary).remove(empl);
			empl.salary = newSalary;
			employeesSalary.computeIfAbsent(empl.salary, k -> new LinkedList<Employee>()).add(empl);
			return ReturnCode.OK;
		} finally {
			readLocks[0].unlock();
			writeLocks[2].unlock();
		}
	}

	@Override
	public ReturnCode updateDepartment(long id, String newDepartment) {
		try {
			readLocks[0].lock();
			Employee empl = mapEmployees.get(id);
			if (empl == null) {
				return ReturnCode.EMPLOYEE_NOT_FOUND;
			}
			if (empl.department.equals(newDepartment)) {
				return ReturnCode.DEPARTMENT_NOT_UPDATED;
			}
			readLocks[0].unlock();
			writeLocks[3].lock();
			employeesDepartment.get(empl.department).remove(empl);
			empl.department = newDepartment;
			employeesDepartment.computeIfAbsent(empl.department, k -> new LinkedList<Employee>()).add(empl);
			return ReturnCode.OK;
		} finally {
			readLocks[0].unlock();
			writeLocks[3].unlock();
		}
	}

	@Override
	public void restore() {
		File inputFile = new File(fileName);
		if (inputFile.exists()) {
			try (ObjectInputStream input = new ObjectInputStream(new FileInputStream(inputFile))) {
				EmployeesMethodsMapsImpl employeesFromFile = (EmployeesMethodsMapsImpl) input.readObject();
				this.employeesAge = employeesFromFile.employeesAge;
				this.employeesDepartment =  employeesFromFile.employeesDepartment;
				this.employeesSalary = employeesFromFile.employeesSalary;
				this.mapEmployees = employeesFromFile.mapEmployees;
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage());
			} 
		}
		
	}

	@Override
	public void save() {
		try(ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(fileName))) {
			writeLocks[0].lock();
			writeLocks[1].lock();
			writeLocks[2].lock();
			writeLocks[3].lock();
			output.writeObject(this);
		} catch (Exception e) {
			throw new RuntimeException(e.toString());
		} finally {
			writeLocks[0].unlock();
			writeLocks[1].unlock();
			writeLocks[2].unlock();
			writeLocks[3].unlock();
		}
	}

}
